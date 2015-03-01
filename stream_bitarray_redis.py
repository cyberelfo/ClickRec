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
frequents = {}
num_transactions = 0
start_t = stop_t = 0
window_timestamp = [0] * window_size
support = args.support / 100.0
next_generate_fis = None
window_id = 0

# def window_id_generator():
#     window_id = 0
#     while True:
#         yield window_id
#         window_id += 1
# 
# window_id_gen = window_id_generator()

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

def user_visit_document(user_index, document_id):
    pages_users[user_index].add(document_id)

    pipe1.setbit("DOCID:"+document_id, user_index, 1)

def zero_column(user_index):
    for doc_id in pages_users[user_index]:
        pipe1.setbit("DOCID:"+doc_id, user_index, 0)

def replace_user(user_id, timestamp):
    global target_user

    if keep_heavy_users:
        candidate_1_index = target_user
        candidate_2_index = (target_user + 1) % window_size

        candidate_1_count = len(pages_users[candidate_1_index])
        candidate_2_count = len(pages_users[candidate_2_index])

        if candidate_1_count > candidate_2_count:
            target_user = candidate_2_index

    removed_user = users[target_user]
    try:
        del users_dict[removed_user]
    except KeyError:
        removed_user = None

    if window_full:
        zero_column(target_user)

    users[target_user] = user_id
    pages_users[target_user] = set()
    users_dict[user_id] = target_user
    window_timestamp[target_user] = timestamp
    target_user = (target_user + 1) % window_size
    return removed_user

def slide_window(size, document_id, user_id, timestamp):
    global window_full

    if user_id not in users_dict:

        removed_user = replace_user(user_id, timestamp)
        window_full = len(users_dict) == window_size

    user_index = users_dict[user_id]
    user_visit_document(user_index, document_id)

def recommend(document_id):
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

def annotations_to_redis():
    print
    print "Saving annotations to Redis"
    total_docs = r.zcard('DOC_COUNTS')
    miss = 0
    bar = Bar('Progress', max=total_docs)
    for k in r.zrevrange('DOC_COUNTS', 0, -1):
        document_id = k[6:]
        if not r.exists("ANNO:"+document_id):

            miss += 1

            annotations = get_annotations(document_id)

            r.rpush("ANNO:"+document_id, '0')
            for i in annotations:
                r.rpush("ANNO:"+document_id, i)

        bar.next()
    bar.finish()
    print "Miss:", miss

def get_annotations(document_id):

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
    local matches = redis.call('KEYS', 'DOCID:*')

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
    

def generate_fis(frequent_size, prev_frequents, max_fi_size, 
    timestamp_generate_fis, document_id):
    global window_id, topten
    print 

    cur_window_size = len(users_dict)

    topten = [i[6:] for i in r.zrevrange('DOC_COUNTS', 0, 9)]

    if document_id == 0:
        cur_support = support
        recursive_generate_fis(frequent_size, prev_frequents, 
            cur_window_size, max_fi_size, cur_support, document_id)
    elif r.exists("DOCID:"+document_id):
        print "document_id:", document_id
        # import pdb; pdb.set_trace()

        upper_bound = r.bitcount("DOCID:"+document_id)
        lower_bound = 1
        while True:
            if upper_bound == lower_bound:
                cur_support = 0
                break
            support_count = (upper_bound + lower_bound) / 2
            print "Support Count:", support_count
            cur_support = float(support_count) / float(cur_window_size)
            if support_count == 1:
                break
            recursive_generate_fis(frequent_size, prev_frequents, 
                cur_window_size, max_fi_size, cur_support, document_id)
            print "Size:", len(frequents[2])
            if len(frequents[2]) >= 5 and len(frequents[2]) <= 10:
                break
            elif len(frequents[2]) > 10:
                lower_bound = support_count + 1
            elif len(frequents[2]) < 5:
                upper_bound = support_count

    else:
        print "document_id:", document_id, "not found"
        return


    # import pdb; pdb.set_trace()

    # Print all frequents
    timestamp_start_pos = window_timestamp[(target_user)%len(users_dict)]
    timestamp_end_pos   = window_timestamp[target_user-1]
    for frequent_size, itemsets in frequents.items():
        if frequent_size > 1 and frequent_size <= max_fi_size:
            print "Window from ", dt.fromtimestamp(int(timestamp_start_pos[:10])), \
                "to",dt.fromtimestamp(int(timestamp_end_pos[:10]))
            print "Support:", cur_support, "- Support count:", cur_support * cur_window_size
            print frequent_size, itemsets
            print "Total itemsets:", len(itemsets)
            print

            if save_results:
                save_frequents(window_id, timestamp_start_pos, timestamp_end_pos, \
                    cur_window_size, support, itemsets, frequent_size, timestamp_generate_fis)

    execution_time = calculate_interval()
    window_id += 1
    print "Execution time:", execution_time
    print

    # import pdb; pdb.set_trace()


def recursive_generate_fis(frequent_size, prev_frequents,
    cur_window_size, max_fi_size, cur_support, document_id):

    frequents[frequent_size] = []

    # import pdb; pdb.set_trace()

    if frequent_size == 1:
        print "Before range"
        list_frequent = r.zrangebyscore("DOC_COUNTS", cur_support * cur_window_size, cur_window_size)

        print "After range"
        for doc_id in list_frequent:
            frequents[frequent_size].append(frozenset([doc_id[6:]]))

    else:
        print "Before size 2"
        item_combinations = set([i.union(j) for i in frequents[frequent_size-1] 
                                            for j in frequents[frequent_size-1] 
                                            if len(i.union(j)) == frequent_size
                                            and i != j])
        if document_id != 0:
            item_combinations = [i for i in item_combinations if document_id in i]
        print "Combinations:", len(item_combinations)
        for itemset in item_combinations:
            for item in enumerate(itemset):
                if item[0] == 0:
                    bit_vector = r.get("DOCID:"+item[1])
                    r.set('bit_vector', bit_vector)
                else:
                    r.bitop('AND', 'bit_vector', 'bit_vector', "DOCID:"+item[1])
            if r.bitcount('bit_vector') >= cur_support * cur_window_size:
                frequents[frequent_size].append(itemset)
        r.delete('bit_vector')

    if len(frequents[frequent_size]) > 0 and frequent_size < max_fi_size:
        recursive_generate_fis(frequent_size+1, frequents[frequent_size],
            cur_window_size, max_fi_size, cur_support, document_id)

def calculate_interval():
    global start_t, stop_t
    stop_t = dt.now()
    execution_time = stop_t - start_t
    start_t = stop_t
    return execution_time

def print_progress(timestamp):
    row_datetime = dt.fromtimestamp(int(timestamp[:10]))
    execution_time = calculate_interval()
    if window_full:
        cur_window_size = window_size
    else:
        cur_window_size = target_user

    print num_transactions, "- Execution time:", \
        execution_time, \
        "Window size:", cur_window_size, "Pages:", \
        "0", "Row timestamp:", row_datetime, "Users", len(users_dict)

def process_stream_file(stream_sorted, selected_product_id):
    global num_transactions, start_t, stop_t, next_generate_fis
    start_t = stop_t = dt.now()
    for product_id, _type, document_id, provider_id, user_id, timestamp  in stream_sorted:
        if product_id == selected_product_id:
            num_transactions += 1
            cur_datetime = dt.fromtimestamp(int(timestamp[:10]))
            slide_window(window_size, document_id, int(user_id), timestamp)
            if num_transactions % 1000 == 0:
                pipe1.execute()
                print_progress(timestamp)
            if window_full and next_generate_fis == None:
                next_generate_fis = next_round_datetime(cur_datetime, generate_fis_interval)
            if window_full and cur_datetime >= next_generate_fis:
                pipe1.execute()
                print_progress(timestamp)

                count_and_delete_pages()
                annotations_to_redis()

                if support == 0:
                    generate_fis(1, [], max_fi_size, next_generate_fis, 
                        document_id)
                else:
                    generate_fis(1, [], max_fi_size, next_generate_fis, 0)
                # fis_analize()
                next_generate_fis = next_round_datetime(cur_datetime + timedelta(seconds=1), generate_fis_interval)
                recommend(document_id)
                
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

def main():
    global cursor, db, execution_id, s, r, pipe1
    print "Program start..."

    start = dt.now()

    db = MySQLdb.connect("localhost","root","","stream" )
    cursor = db.cursor()

    s = solr.SolrConnection(solr_endpoint)

    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    r.flushdb()
    pipe1 = r.pipeline()

    if save_results:
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
