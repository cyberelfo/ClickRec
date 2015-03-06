#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import argparse
import ConfigParser
from datetime import datetime as dt
from datetime import timedelta
import glob
import solr
import redis
from progress.bar import Bar

config = ConfigParser.ConfigParser()
config.read("./stream.ini")

path = config.get('main', 'path')
filename = config.get('main', 'filename')
local_file = config.getboolean('main', 'local_file')
solr_endpoint = config.get('main', 'solr_endpoint')
selected_product_id = config.get('main', 'product_id')

parser = argparse.ArgumentParser()
parser.add_argument("max_files", nargs='?', default=0, type=int, 
    help="Number of transactions to load, 0 = all")
args = parser.parse_args()

max_files = args.max_files

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

def clean_redis():

    # lua = "redis.call('del', unpack(redis.call('keys', 'DOCID:*')))"

    lua = """
    local matches = redis.call('KEYS', 'ANNO:*')

    for _,key in ipairs(matches) do
        redis.call('DEL', key)
    end
    """    

    delete_annotations = r.register_script(lua)

    delete_annotations()

def process_stream_file(stream, selected_product_id):
    start_t = stop_t = dt.now()
    num_transactions = 0
    miss = 0
    for product_id, _type, document_id, provider_id, user_id, timestamp  in stream:
        if product_id == selected_product_id:
            num_transactions += 1

            miss += annotations_to_redis(document_id)
            if num_transactions % 1000 == 0:
                # pipe1.execute()
                stop_t = dt.now()
                delta_t = stop_t - start_t
                row_datetime = dt.fromtimestamp(int(timestamp[:10]))
                print "Click count:", num_transactions, "Timestamp:", row_datetime, "Elapsed:", delta_t, "miss:", miss
                miss = 0
                start_t = dt.now()
                # import pdb; pdb.set_trace()

def annotations_to_redis(document_id):
    total_docs = r.zcard('DOC_COUNTS')
    annotation_found = r.exists("ANNO:"+document_id)
    if not annotation_found:
        response = s.query('documentId:'+str(document_id), fields='entity')

        annotations = ['0']
        if response.numFound == 1:
            if 'entity' in response.results[0]:
                annotations = response.results[0]['entity']

        # if annotations == ['0']:
        #     print "ANNO:"+document_id
        r.rpush("ANNO:"+document_id, *annotations)

    return not annotation_found


def get_files(local_file):
    if local_file:
         file_list = glob.glob(path + "*.log")
    else:
        file_list = None
    file_list.sort()
    return file_list


def main():
    print "Program start..."
    global r, s

    start = dt.now()

    s = solr.SolrConnection(solr_endpoint)

    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    file_list = get_files(local_file)

    print "Clean Redis..."

    clean_redis()

    for num_file, filename in enumerate(file_list):
        if num_file >= max_files:
            break
        print ""
        print "Reading stream file "+ filename + "..."

        f = open(filename, 'rb')

        stream = csv.reader(f)

        print "Sorting stream..."

        stream_sorted = sort_by_column(stream, 5)

        process_stream_file(stream_sorted, selected_product_id)

        f.close()

    stop = dt.now()
    execution_time = stop - start 

    print "End processing"
    print "Execution time:", execution_time


if __name__ == '__main__':
    main()
    # import cProfile
    # cProfile.run('main()')
