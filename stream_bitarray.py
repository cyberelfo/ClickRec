#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
from itertools import combinations
from pprint import pprint
from bitarray import bitarray
import argparse
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta
import requests
import glob
import MySQLdb
import solr


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

hadoop_server = config.get('hadoop', 'hadoop_server')
hadoop_port = config.get('hadoop', 'hadoop_port')
hadoop_path = config.get('hadoop', 'hadoop_path')

parser = argparse.ArgumentParser()

parser.add_argument("window_size", type=int, 
    help="Number of users / size of the window")
parser.add_argument("support", type=float, 
    help="Support in percent of transactions [1-100]")
parser.add_argument("max_transactions", nargs='?', default=0, type=int, 
    help="Number of transactions to load, 0 = all")
args = parser.parse_args()

# Global variables

window_size = args.window_size
window_full = False
target_user = 0
max_transactions = args.max_transactions
bit_array = {} # armazenar os bit_vectors de todos os documentos
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

def read_from_hadoop(filename):
    from StringIO import StringIO
    # import pdb; pdb.set_trace()

    url = "http://" + hadoop_server + ":" \
          + hadoop_port + "/webhdfs/v1/" + hadoop_path \
          + filename
    filename = url.split('/')[-1]
    r = requests.get(url, params={'op':'OPEN'})
    s = StringIO(r.content)
    r.close()
    return s

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
    try:
        bit_array[document_id][user_index] = True
    except KeyError:
        bit_array[document_id] = bitarray([False] * window_size)
        bit_array[document_id][user_index] = True

def zero_column(user_index, bit_array):
    for doc_id in pages_users[user_index]:
        bit_array[doc_id][user_index] = False
        if bit_array[doc_id].count() == 0:
            del bit_array[doc_id]


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
    zero_column(target_user, bit_array)

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

def get_url_solr(document_id):
    global s
    response = s.query('documentId:'+str(document_id), fl="url")
    if response.numFound == 1:
        url = response.results[0]['url']
        title = response.results[0]['title']
        publish_date = response.results[0]['issued']
        modify_date = response.results[0]['modified']
        section = response.results[0]['section'][0]

        # import pdb; pdb.set_trace()

        # body = response.results[0]['body']
        body = ""
        publisher = response.results[0]['publisher']
        # print result[0], "-", url
    else:
        url = None
    return url

def save_frequents(window_id, timestamp_start_pos, timestamp_end_pos,
            cur_window_size, support, itemsets, frequent_size, timestamp_generate_fis):
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
        for document_id in itemset:
            url = get_url_solr(document_id)
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

def generate_fis(frequent_size, prev_frequents, bit_array, max_fi_size, timestamp_generate_fis):
    global window_id
    print 

    cur_window_size = len(users_dict)
    if remove_bounce_users:
        bit_array = {k:v.copy() for k, v in bit_array.items()}
        bounce_users = [user_index for user_index, v in enumerate(pages_users) if len(v)==1]
        for user_index in bounce_users:
            zero_column(user_index, bit_array)

        cur_window_size -= len(bounce_users)
        print "Usuários removidos:", len(bounce_users), "Window size:", cur_window_size

    recursive_generate_fis(frequent_size, prev_frequents, bit_array, cur_window_size, max_fi_size)

    # Print all frequents
    timestamp_start_pos = window_timestamp[(target_user)%cur_window_size]
    timestamp_end_pos   = window_timestamp[target_user-1]
    for frequent_size, itemsets in frequents.items():
        if frequent_size > 1 and frequent_size <= max_fi_size:
            print "Window from ", dt.fromtimestamp(int(timestamp_start_pos[:10])), \
                "to",dt.fromtimestamp(int(timestamp_end_pos[:10]))
            print "Support:", support * cur_window_size
            print frequent_size, itemsets

            if save_results:
                save_frequents(window_id, timestamp_start_pos, timestamp_end_pos, \
                    cur_window_size, support, itemsets, frequent_size, timestamp_generate_fis)

    execution_time = calculate_interval()
    window_id += 1
    print "execution time:", execution_time
    print


def recursive_generate_fis(frequent_size, prev_frequents, bit_array,\
    cur_window_size, max_fi_size):

    frequents[frequent_size] = []

    if frequent_size == 1:          
        for doc_id in bit_array.keys():
            if bit_array[doc_id].count() >= support * cur_window_size:
                frequents[frequent_size].append(doc_id)
    else:
        # import pdb; pdb.set_trace()
        if frequent_size == 2:
            prev_freq_split = prev_frequents
        else:
            prev_freq_split = set([item for sublist in prev_frequents for item in sublist])
        item_combinations = list(combinations(prev_freq_split, frequent_size))
        for itemset in item_combinations:
            for item in enumerate(itemset):
                if item[0] == 0:
                    bit_vector = bit_array[item[1]]
                else:
                    bit_vector = bit_vector & bit_array[item[1]]
            if bit_vector.count() >= support * cur_window_size:
                frequents[frequent_size].append(itemset)


    if len(frequents[frequent_size]) > 0 and frequent_size < max_fi_size:
        # import pdb; pdb.set_trace()
        recursive_generate_fis(frequent_size+1, frequents[frequent_size], bit_array, \
            cur_window_size, max_fi_size)

def debug_array(user_id, document_id, target_user):
    print ""
    print "User:", user_id, "[", target_user,"]"
    print "Document:", document_id
    pprint(bit_array)

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
        len(bit_array), "Row timestamp:", row_datetime, "Users", len(users_dict)

def process_stream_file(stream_sorted, selected_product_id):
    global num_transactions, start_t, stop_t, next_generate_fis
    start_t = stop_t = dt.now()
    for product_id, _type, document_id, provider_id, user_id, timestamp  in stream_sorted:
        if product_id == selected_product_id:
            # import pdb; pdb.set_trace()
            num_transactions += 1
            cur_datetime = dt.fromtimestamp(int(timestamp[:10]))
            if max_transactions > 0 and num_transactions > max_transactions: 
                break

            slide_window(window_size, int(document_id), int(user_id), timestamp)

            if num_transactions % 1000 == 0:
                print_progress(timestamp)
            # if num_transactions % 10000 == 0 and window_full:
            #     generate_fis(1, [], bit_array, max_fi_size)
            if window_full and next_generate_fis == None:
                next_generate_fis = next_round_datetime(cur_datetime, 5)
            if window_full and cur_datetime >= next_generate_fis:
                print_progress(timestamp)
                generate_fis(1, [], bit_array, max_fi_size, next_generate_fis)
                next_generate_fis = next_round_datetime(cur_datetime + timedelta(seconds=1), 5)
            # debug_array(user_id, document_id, target_user)

def get_files(local_file):
    if local_file:
         file_list = glob.glob(path + "*.log")
    else:
        file_list = None
    file_list.sort()
    return file_list

def main():
    global cursor, db, execution_id, s
    print "Program start..."

    start = dt.now()

    db = MySQLdb.connect("localhost","root","","stream" )
    cursor = db.cursor()

    s = solr.SolrConnection(solr_endpoint)

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

    for filename in file_list:
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

        if max_transactions > 0 and num_transactions > max_transactions: 
            break

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
