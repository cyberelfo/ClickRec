#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import argparse
import ConfigParser
from datetime import datetime
import glob
import solr
from pprint import pprint
from progress.bar import Bar

config = ConfigParser.ConfigParser()
config.read("./stream.ini")

path = config.get('main', 'path')
filename = config.get('main', 'filename')
product_id = config.get('main', 'product_id')
solr_endpoint = config.get('main', 'solr_endpoint')

parser = argparse.ArgumentParser()
parser.add_argument("max_files", type=int,
    help="Number of files to process. 0 = all")
args = parser.parse_args()

max_files = args.max_files

def get_files():
    file_list = glob.glob(path + "*.log")
    file_list.sort()
    return file_list

def process_stream():
    start_stream = start_file = start_10k = datetime.now()

    users = {}
    num_transactions = 0

    file_list = get_files()

    filecount = 0
    for filename in file_list:
        print "Reading file", filename, "..."
        f = open(filename, 'r')
        stream = csv.reader(f)

        for cur_product_id, _type, document_id, provider_id, user_id, timestamp in stream:
            num_transactions += 1
            if num_transactions % 100000 == 0:
                elapsed_10k = datetime.now() - start_10k
                start_10k = datetime.now()
                print elapsed_10k, num_transactions

            if user_id not in users:
                users[user_id] = [document_id]
            else:
                users[user_id].append(document_id)

        elapsed_file = datetime.now() - start_file
        start_file = datetime.now()
        print "File time:", elapsed_file
        print 

        filecount += 1
        if max_files > 0 and filecount >= max_files:
            break

    elapsed_stream = datetime.now() - start_stream
    start_stream = datetime.now()
    print
    print "All files time:", elapsed_stream, "Total transactions:", num_transactions
    print

    return users

def count_users(users):
    start_count = datetime.now()
    bar = Bar('Progress', max=len(users)/1000)
    users_count = {}
    counter = 0
    for user_id, pages_visited in users.items():
        counter += 1
        if counter % 1000 == 0:
            bar.next()
        pages_count = len(pages_visited)
        if pages_count not in users_count:
            users_count[pages_count] = 1
        else:
            users_count[pages_count] += 1

    bar.finish()
    pprint(users_count)
    elapsed = datetime.now() - start_count
    print
    print "Counting total time:", elapsed
    print

def get_section_solr(document_id):
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
        section = None
    return section

def model_path(users, max_path):
    start_model = datetime.now()
    user_model = {}

    bar = Bar('Progress', max=100)

    counter = 0
    for user_id, pages_visited in users.items():
        pages_count = len(pages_visited)
        if pages_count == 4:
            counter += 1
            bar.next()
            section_list = []
            for page in pages_visited:
                section_list.append(get_section_solr(page))
            user_model[user_id] = section_list
        if counter >= 100:
            break

    bar.finish()

    elapsed = datetime.now() - start_model
    pprint(user_model)
    print
    print "Model total time:", elapsed
    print


def main():
    global s
    s = solr.SolrConnection(solr_endpoint)

    start = datetime.now()

    users = process_stream()
    count_users(users)
    model_path(users, 5)

if __name__ == '__main__':
    main()
