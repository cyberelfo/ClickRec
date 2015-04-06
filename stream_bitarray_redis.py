#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
from itertools import combinations
import argparse
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta
import glob
import MySQLdb
import solr
import redis
from bitarray import bitarray
import requests
from progress.bar import Bar
import pickle

config = ConfigParser.ConfigParser()
config.read("./stream.ini")

path = config.get('main', 'path')
filename = config.get('main', 'filename')
local_file = config.getboolean('main', 'local_file')
save_results = config.getboolean('main', 'save_results')
solr_endpoint = config.get('main', 'solr_endpoint')
selected_product_id = config.get('main', 'product_id')
keep_heavy_users = config.getboolean('main', 'keep_heavy_users')
remove_bounce_users = config.getboolean('main', 'remove_bounce_users')
max_fi_size = config.getint('main', 'max_fi_size')
generate_fis_interval = config.getint('main', 'generate_fis_interval')

hadoop_server = config.get('hadoop', 'hadoop_server')
hadoop_port = config.get('hadoop', 'hadoop_port')
hadoop_path = config.get('hadoop', 'hadoop_path')

brainiak_server = config.get('brainiak', 'brainiak_server')

parser = argparse.ArgumentParser()

parser.add_argument("window_size", type=int, 
    help="Number of users / size of the window")
parser.add_argument("support", type=float, 
    help="Support in percent of transactions [1-100]")
parser.add_argument("max_files", nargs='?', default=0, type=int, 
    help="Number of transactions to load, 0 = all")
args = parser.parse_args()

# Global variables

window_size = args.window_size
window_full = False
target_user = 0
max_files = args.max_files
users = [0] * window_size
users_dict = {}
pages_users = [set() for i in range(window_size)]
annotations_users = [set() for i in range(window_size)]
sections_users = [set() for i in range(window_size)]
frequents = {}
num_transactions = 0
start_t = stop_t = 0
window_timestamp = [0] * window_size
support = args.support / 100.0
next_generate_fis = None
window_id = 0

def next_round_datetime(cur_datetime, interval):
    #how many secs have passed this hour
    nsecs = cur_datetime.minute*60+cur_datetime.second+cur_datetime.microsecond*1e-6  
    #number of seconds to next quarter hour mark
    #Non-analytic (brute force is fun) way:  
    #   delta = next(x for x in xrange(0,3601,900) if x>=nsecs) - nsecs
    #anlytic (ARGV BATMAN!, what is going on with that expression) way:
    delta = (nsecs//(interval*60))*(interval*60)+(interval*60)-nsecs
    #time + number of seconds to quarter hour mark.
    return cur_datetime + timedelta(seconds=delta)

def sort_by_column(csv_cont, col_index, reverse=False):
    """ 
    Sorts CSV contents by column index (if col_index argument 
    is type <int>). 
    
    """
    import operator

    body = csv_cont
    body = sorted(body, 
           key=operator.itemgetter(col_index), 
           reverse=reverse)
    return body

def user_visit_document(user_index, document_id, annotations, sections):
    pages_users[user_index].add(document_id)
    pipe1.setbit("BIT_DOC:"+document_id, user_index, 1)

    annotations_users[user_index] |= set(annotations)
    for uri in annotations:
        pipe1.setbit("BIT_URI:"+uri, user_index, 1)

    sections_users[user_index] |= set(sections)
    for section in sections:
        pipe1.setbit("BIT_SEC:"+section, user_index, 1)

def zero_column(user_index):
    for doc_id in pages_users[user_index]:
        pipe1.setbit("BIT_DOC:"+doc_id, user_index, 0)

    for uri in annotations_users[user_index]:
        pipe1.setbit("BIT_URI:"+uri, user_index, 0)

    for section in sections_users[user_index]:
        pipe1.setbit("BIT_SEC:"+section, user_index, 0)

def replace_user(user_id, timestamp):
    global target_user

    removed_user = users[target_user]
    try:
        del users_dict[removed_user]
    except KeyError:
        removed_user = None

    if window_full:
        zero_column(target_user)

    users[target_user] = user_id
    pages_users[target_user] = set()
    annotations_users[target_user] = set()
    sections_users[target_user] = set()
    users_dict[user_id] = target_user
    window_timestamp[target_user] = timestamp
    target_user = (target_user + 1) % window_size
    return removed_user

def slide_window(size, document_id, user_id, timestamp, annotations, sections):
    global window_full

    if user_id not in users_dict:

        removed_user = replace_user(user_id, timestamp)
        window_full = len(users_dict) == window_size

    user_index = users_dict[user_id]
    user_visit_document(user_index, document_id, annotations, sections)

def recommend_by_pages(document_id):
    recommendations = set()
    for itemset in frequents[2]:
        if document_id in itemset:
            # import pdb; pdb.set_trace()
            item = set(itemset) - set([document_id])
            recommendations = recommendations.union(item)

    # import pdb; pdb.set_trace()

    for docid in topten:
        if len(recommendations) >= 10:
            break
        if docid != document_id:
            recommendations = recommendations.union([docid])

    print "Document:", document_id
    print "Recommendations:", recommendations
    print

    # import pdb; pdb.set_trace()

def recommend_by_uris(document_id):
    recommendations = set()

    miss, annotations, sections = get_annotations(document_id)
    # import pdb; pdb.set_trace()
    if annotations == ['0']:
        return recommendations

    # annotations_combinations = [set(i) for i in combinations(annotations, 2)]

    for annotation in annotations:
        for frequent in frequents[2]:
            if annotation in frequent:
                recommendations.add(frequent - set([annotation]))

    return recommendations

def get_url_solr(document_id):
    global s

    if r.exists("SOLR:"+document_id):
        url  = r.get("SOLR:"+document_id)
    else:
        response = s.query('documentId:'+str(document_id), fields='url')

        # import pdb; pdb.set_trace()
        if response.numFound == 1:
            url = response.results[0]['url']
            # publisher = response.results[0]['publisher']
        else:
            url = None

        r.set("SOLR:"+document_id, url)

    return url


def get_annotations(document_id):

    annotation_found = r.exists("ANNOTATIONS:"+document_id)
    if annotation_found:
        annotations = r.lrange("ANNOTATIONS:"+document_id, 0, -1)
        sections = r.lrange("SECTIONS:"+document_id, 0, -1)
        miss = 0

    else:
        response = s.query('documentId:'+str(document_id), fields='entity, section')

        annotations = ['0']
        sections = ['0']

        if response.numFound == 1:
            if 'entity' in response.results[0]:
                annotations = response.results[0]['entity']
            if 'section' in response.results[0]:
                sections = response.results[0]['section']

        # if annotations == ['0']:
        #     print "ANNOTATIONS:"+document_id
        r.rpush("ANNOTATIONS:"+document_id, *annotations)
        r.rpush("SECTIONS:"+document_id, *sections)
        miss = 1

    return miss, annotations, sections

def get_annotations_from_brainiak(document_id):

    permalink = get_url_solr(document_id)

    url = "http://" + brainiak_server + ":" \
          + "/_query/annotation_from_permalink/_result"

    response = requests.get(url, params={'permalink':permalink})

    json = response.json()

    annotations = []
    if response.ok:
        for j in json['items']:
            annotations.append(j['label'])

    return annotations

def count_and_delete_pages():

    lua = """
    local matches = redis.call('KEYS', 'BIT_DOC:*')

    redis.call('DEL', 'DOC_COUNTS')

    for _,key in ipairs(matches) do
        local val = redis.call('BITCOUNT', key)
        if val == 0 then
            redis.call('DEL', key)
        else
            redis.call('ZADD', 'DOC_COUNTS', tonumber(val), key)
        end
    end
    """    

    count_pages = r.register_script(lua)
    count_pages()
    
def count_and_delete_uris():

    lua = """
    local matches = redis.call('KEYS', 'BIT_URI:*')

    redis.call('DEL', 'URI_COUNTS')

    for _,key in ipairs(matches) do
        local val = redis.call('BITCOUNT', key)
        if val == 0 then
            redis.call('DEL', key)
        elseif key == "BIT_URI:0" then
        -- no actions if key is "BIT_URI:0"
        else
            redis.call('ZADD', 'URI_COUNTS', tonumber(val), key)
        end
    end
    """    

    count_uris = r.register_script(lua)
    count_uris()

def count_and_delete_sections():

    lua = """
    local matches = redis.call('KEYS', 'BIT_SEC:*')

    redis.call('DEL', 'SEC_COUNTS')

    for _,key in ipairs(matches) do
        local val = redis.call('BITCOUNT', key)
        if val == 0 then
            redis.call('DEL', key)
        elseif key == "BIT_SEC:0" then
        -- no actions if key is "BIT_SEC:0"
        else
            redis.call('ZADD', 'SEC_COUNTS', tonumber(val), key)
        end
    end
    """    

    count_uris = r.register_script(lua)
    count_uris()

def save_frequents(window_id, timestamp_start_pos, timestamp_end_pos,
    cur_window_size, support, itemsets, frequent_size, timestamp_generate_fis, pages_or_uris):
    global selected_product_id

    cursor.execute(""" insert into bitstream_windows 
        (execution_id, window_id, window_timestamp, window_start, window_end, window_size
            )
        values (%s, %s, %s, %s, %s, %s);
        """, [execution_id, window_id, timestamp_generate_fis,
                dt.fromtimestamp(int(timestamp_start_pos[:10])),
                dt.fromtimestamp(int(timestamp_end_pos[:10])),
                cur_window_size
                ] )

    for itemset_id, itemset in enumerate(itemsets):
        for item in itemset:

            if pages_or_uris == 'pages':
                document_id = item
                url = get_url_solr(document_id)
            else:
                document_id = 0
                url = item

            cursor.execute(""" insert into bitstream_itemsets 
                (execution_id, product_id, window_id, window_timestamp, window_start, window_end, window_size, support,
                    itemset_id, itemset_size, document_id, keep_heavy_users, remove_bounce_users, url
                    )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, [execution_id, selected_product_id, window_id, 
                        timestamp_generate_fis,
                        dt.fromtimestamp(int(timestamp_start_pos[:10])),
                        dt.fromtimestamp(int(timestamp_end_pos[:10])),
                        cur_window_size, support, itemset_id, frequent_size,
                        document_id, keep_heavy_users, remove_bounce_users, url
                        ] )

    db.commit()


def check_uris(itemsets):
    topten_docs = [i[len('BIT_DOC:'):] for i in r.zrevrange('DOC_COUNTS', 0, 9)]

    hit_top_docs = 0
    for itemset in itemsets:
        for doc_id in topten_docs:
            miss, annotations, sections = get_annotations(doc_id)
            if itemset.issubset(annotations):
                hit_top_docs += 1
                # import pdb; pdb.set_trace()
                break

    print "hit_top_docs:", hit_top_docs

def save_frequents_redis(pages_or_uris, fi_size):
    r.delete('FREQS:'+pages_or_uris+':'+str(fi_size))
    pickled_object = pickle.dumps(frequents[fi_size])
    r.set('FREQS:'+pages_or_uris+':'+str(fi_size), pickled_object)

def generate_fis(frequent_size, prev_frequents, max_fi_size, 
    timestamp_generate_fis, pages_or_uris):
    global window_id, topten
    print 

    print "Pages or URIs?", pages_or_uris
    print 

    if pages_or_uris == 'pages':
        keys_prefix = 'BIT_DOC:'
        counts_prefix = 'DOC_COUNTS'
    elif pages_or_uris == 'uris':
        keys_prefix = 'BIT_URI:'
        counts_prefix = 'URI_COUNTS'
    else:
        keys_prefix = 'BIT_SEC:'
        counts_prefix = 'SEC_COUNTS'

    len_prefix = len(keys_prefix)

    cur_window_size = len(users_dict)

    topten = [i[len_prefix:] for i in r.zrevrange(counts_prefix, 0, 9)]

    cur_support = support
    recursive_generate_fis(frequent_size, prev_frequents, 
        cur_window_size, max_fi_size, cur_support, pages_or_uris)

    # import pdb; pdb.set_trace()

    # Print all frequents
    timestamp_start_pos = window_timestamp[(target_user)%len(users_dict)]
    timestamp_end_pos   = window_timestamp[target_user-1]

    print "Window from ", dt.fromtimestamp(int(timestamp_start_pos[:10])), \
        "to",dt.fromtimestamp(int(timestamp_end_pos[:10]))
    print "Support:", cur_support, "- Support count:", cur_support * cur_window_size

    for frequent_size, itemsets in frequents.items():
        print
        print "Total itemsets size [" + str(frequent_size) + "]:", len(itemsets)
        # if frequent_size > 1:
        #     check_uris(itemsets)

        if save_results:
            save_frequents(window_id, timestamp_start_pos, timestamp_end_pos,
                cur_window_size, support, itemsets, frequent_size, timestamp_generate_fis,
                pages_or_uris)


    save_frequents_redis(pages_or_uris, 2)
    print
    execution_time = calculate_interval()
    window_id += 1
    print "Execution time:", execution_time
    print

    # import pdb; pdb.set_trace()


def recursive_generate_fis(frequent_size, prev_frequents,
    cur_window_size, max_fi_size, cur_support, pages_or_uris):

    frequents[frequent_size] = []

    if pages_or_uris == 'pages':
        keys_prefix = 'BIT_DOC:'
        counts_prefix = 'DOC_COUNTS'
    elif pages_or_uris == 'uris':
        keys_prefix = 'BIT_URI:'
        counts_prefix = 'URI_COUNTS'
    else:
        keys_prefix = 'BIT_SEC:'
        counts_prefix = 'SEC_COUNTS'

    len_prefix = len(keys_prefix)

    if frequent_size == 1:
        list_frequent = r.zrangebyscore(counts_prefix, cur_support * cur_window_size, cur_window_size)

        for doc_id in list_frequent:
            frequents[frequent_size].append(frozenset([doc_id[len_prefix:]]))

    else:
        item_combinations = set([i.union(j) for i in frequents[frequent_size-1] 
                                            for j in frequents[frequent_size-1] 
                                            if len(i.union(j)) == frequent_size
                                            and i != j])
        print "Combinations size[" + str(frequent_size) + "]:", len(item_combinations)


        for itemset in item_combinations:
            for item in enumerate(itemset):
                if item[0] == 0:
                    bit_vector = r.get(keys_prefix+item[1])
                    r.set('bit_vector', bit_vector)
                else:
                    r.bitop('AND', 'bit_vector', 'bit_vector', keys_prefix+item[1])
            if r.bitcount('bit_vector') >= cur_support * cur_window_size:
                frequents[frequent_size].append(itemset)
        r.delete('bit_vector')

    # import pdb; pdb.set_trace()

    if len(frequents[frequent_size]) > 0 and frequent_size < max_fi_size:
        recursive_generate_fis(frequent_size+1, frequents[frequent_size],
            cur_window_size, max_fi_size, cur_support, pages_or_uris)

def calculate_interval():
    global start_t, stop_t
    stop_t = dt.now()
    execution_time = stop_t - start_t
    start_t = stop_t
    return execution_time

def print_progress(timestamp, miss, recommendation_ratio = 0):
    row_datetime = dt.fromtimestamp(int(timestamp[:10]))
    execution_time = calculate_interval()
    if window_full:
        cur_window_size = window_size
    else:
        cur_window_size = target_user

    print num_transactions, "- Execution time:", \
        execution_time, \
        "Window size:", cur_window_size, "Pages:", \
        "0", "Row timestamp:", row_datetime, "Miss:", miss, "Ratio:", recommendation_ratio

def process_stream_file(stream_sorted, selected_product_id):
    global num_transactions, start_t, stop_t, next_generate_fis
    start_t = stop_t = dt.now()
    has_fis = False
    total_recommendations = 0
    total_recommendations_found = 0
    recommendation_ratio = 0
    total_miss = 0
    for product_id, _type, document_id, provider_id, user_id, timestamp  in stream_sorted:
        if product_id == selected_product_id:
            num_transactions += 1
            cur_datetime = dt.fromtimestamp(int(timestamp[:10]))
            miss, annotations, sections = get_annotations(document_id)
            total_miss += miss
            slide_window(window_size, document_id, int(user_id), timestamp, annotations, sections)
            if num_transactions % 1000 == 0:
                pipe1.execute()
                print_progress(timestamp, total_miss, recommendation_ratio)
                total_miss = 0
            if window_full and next_generate_fis == None:
                next_generate_fis = next_round_datetime(cur_datetime, generate_fis_interval)
            if window_full and cur_datetime >= next_generate_fis:
                pipe1.execute()
                print_progress(timestamp, total_miss)
                total_miss = 0

                count_and_delete_pages()
                count_and_delete_uris()
                count_and_delete_sections()

                # generate_fis(1, [], max_fi_size, next_generate_fis, 'pages')
                generate_fis(1, [], max_fi_size, next_generate_fis, 'sections')
                generate_fis(1, [], max_fi_size, next_generate_fis, 'uris')
                # fis_analize()
                has_fis = True
                next_generate_fis = next_round_datetime(cur_datetime + timedelta(seconds=1), generate_fis_interval)
                # recommend_by_pages(document_id)

                total_recommendations = 0
                total_recommendations_found = 0
                recommendation_ratio = 0

            if window_full and has_fis:
                total_recommendations += 1
                recommendation = recommend_by_uris(document_id)
                if len(recommendation) > 0:
                    total_recommendations_found += 1
                recommendation_ratio = float(total_recommendations_found) / float(total_recommendations)

                # import pdb; pdb.set_trace()


def fis_analize():
    frequents_size_2 = set()
    for frequent in frequents[2]:
        for item in frequent:
            frequents_size_2.add(item)
    print "Pages:", list(frequents_size_2)
    print "Total pages:", len(frequents_size_2)
    print


def get_files(local_file):
    if local_file:
         file_list = glob.glob(path + "*.log")
    else:
        file_list = None
    file_list.sort()
    return file_list

def clean_redis():

    lua = """
    local matches = redis.call('KEYS', 'BIT_DOC:*')

    redis.call('DEL', 'DOC_COUNTS')

    for _,key in ipairs(matches) do
        redis.call('DEL', key)
    end

    local matches = redis.call('KEYS', 'BIT_URI:*')

    redis.call('DEL', 'URI_COUNTS')

    for _,key in ipairs(matches) do
        redis.call('DEL', key)
    end

    local matches = redis.call('KEYS', 'BIT_SEC:*')

    redis.call('DEL', 'SEC_COUNTS')

    for _,key in ipairs(matches) do
        redis.call('DEL', key)
    end
    """    

    delete_redis = r.register_script(lua)

    delete_redis()

    lua = """
    local matches = redis.call('KEYS', 'ANNOTATIONS:*')

    for _,key in ipairs(matches) do
        redis.call('DEL', key)
    end

    local matches = redis.call('KEYS', 'SECTIONS:*')

    for _,key in ipairs(matches) do
        redis.call('DEL', key)
    end
    """    

    delete_annotations = r.register_script(lua)

    delete_annotations()


def main():
    global cursor, db, execution_id, s, r, pipe1
    print "Program start..."

    start = dt.now()

    s = solr.SolrConnection(solr_endpoint)

    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    print "Clean Redis..."
    clean_redis()

    pipe1 = r.pipeline()

    if save_results:

        db = MySQLdb.connect("localhost","root","","stream" )
        cursor = db.cursor()

        cursor.execute(""" insert into bitstream_execution 
            (date_execution, product_id, window_size, support, keep_heavy_users, \
                remove_bounce_users, max_fi_size
                )
            values (%s, %s, %s, %s, %s, %s, %s);
            """, [start, selected_product_id, window_size, support, keep_heavy_users,
                remove_bounce_users, max_fi_size] )
        execution_id = cursor.lastrowid
        db.commit()

    file_list = get_files(local_file)

    for num_file, filename in enumerate(file_list):
        if num_file >= max_files:
            break
        print ""
        print "Reading stream file "+ filename + "..."

        if local_file:
            f = open(filename, 'rb')
        else:
            f = read_from_hadoop("rt-actions-read-2015_01_14_12.log")

        stream = csv.reader(f)

        print "Sorting stream..."

        stream_sorted = sort_by_column(stream, 5)

        stop = dt.now()
        execution_time = stop - start 

        print "Stream sorted in:", execution_time 

        process_stream_file(stream_sorted, selected_product_id)

        f.close()

    stop = dt.now()
    execution_time = stop - start 

    if save_results:
        cursor.execute(""" update bitstream_execution 
            set execution_time = %s
            where id = %s;
            """, [execution_time.seconds, execution_id] )
        db.commit()

    print "End processing"
    print "Execution time:", execution_time

if __name__ == '__main__':
    main()
    # import cProfile
    # cProfile.run('main()')
