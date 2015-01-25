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
import MySQLdb

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

all_pages = {}

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
            if num_transactions > 50000:
                break
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
    page_count = {}
    users_count = {}
    counter = 0
    for user_id, pages_visited in users.items():
        counter += 1
        if counter % 1000 == 0:
            bar.next()
        users_count[user_id] = len(pages_visited)
        if users_count[user_id] not in page_count:
            page_count[users_count[user_id]] = 1
        else:
            page_count[users_count[user_id]] += 1

    bar.finish()
    pprint(page_count)
    elapsed = datetime.now() - start_count
    print
    print "Counting total time:", elapsed
    print
    return users_count

def get_site_solr(document_id):
    global s, all_pages
    if document_id in all_pages:
        site = all_pages[document_id]
    else:
        site = None

        response = s.query('documentId:'+str(document_id), fl="site")
        if response.numFound == 1:
            body = ""
            if 'site' in response.results[0]:
                site = response.results[0]['site']
            # import pdb; pdb.set_trace()

        all_pages[document_id] = site

    return site

def model_path(users, users_count):
    start_model = datetime.now()
    total_users = len(users)
    bar = Bar('Progress', max=total_users/100)
    # import pdb; pdb.set_trace()
    all_models = set()
    counter = 0
    for user_id, pages_visited in users.items():
        counter += 1
        if counter % 100 == 0:
            bar.next()
        # pages_count = len(pages_visited)
        # if pages_count > 1 and pages_count <= max_path:
        if users_count[user_id] > 1 :
            site_list = []
            for page in pages_visited:
                pagesite = get_site_solr(page)
                site_list.append(pagesite)
            all_models.add(tuple(site_list))
        # if counter >= 5000:
        #     break

    bar.finish()

    elapsed = datetime.now() - start_model
    print
    print len(all_models)
    print "Model total time:", elapsed
    print
    return all_models

def save_models(all_models):
    global cursor, db
    print "Saving models..."
    db.begin()
    results = []
    for model_id, sites in enumerate(all_models):
        for sequence_id, site in enumerate(sites):
            results.append([sequence_id, model_id, site])

        if model_id % 1000 == 0:
            cursor.executemany(""" insert into users_model 
                (sequence_id, model_id, site_name)
                values(%s, %s, %s)
                """, (results) )
            db.commit()
            db.begin()
            results = []
    if len(results) > 0:
        cursor.executemany(""" insert into users_model 
            (sequence_id, model_id, site_name)
            values(%s, %s, %s)
            """, (results) )
        db.commit()



def main():
    global cursor, db, s
    print "Program start..."

    start = datetime.now()

    db = MySQLdb.connect("localhost","root","","stream" )
    cursor = db.cursor()

    s = solr.SolrConnection(solr_endpoint)

    start = datetime.now()

    users = process_stream()
    users_count = count_users(users)
    all_models = model_path(users, users_count)
    save_models(all_models)

    stop = datetime.now()
    execution_time = stop - start 

    print
    print "End processing"
    print "Execution time:", execution_time


if __name__ == '__main__':
    main()
    # import cProfile
    # cProfile.run('main()')
