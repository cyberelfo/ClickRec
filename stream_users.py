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
save_results = config.getboolean('main', 'save_results')

parser = argparse.ArgumentParser()
parser.add_argument("max_files", type=int,
    help="Number of files to process. 0 = all")
args = parser.parse_args()

max_files = args.max_files

cache_pages = {}

total_hit = 0
total_miss = 0

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
            # if num_transactions > 50000:
            #     break
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
    print "Counting users..."
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

    elapsed = datetime.now() - start_count
    print
    print "Counting total time:", elapsed
    print
    return users_count, page_count

def get_site_solr(document_id):
    global s, cache_pages, total_hit, total_miss
    if document_id in cache_pages:
        site = cache_pages[document_id]
        total_hit += 1
    else:
        total_miss += 1
        site = None

        response = s.query('documentId:'+str(document_id), fl="site")
        if response.numFound == 1:
            body = ""
            if 'site' in response.results[0]:
                site = response.results[0]['site']
            # import pdb; pdb.set_trace()

        cache_pages[document_id] = site

    return site

def model_path(users, users_count):
    start_model = datetime.now()
    print "Generation models..."

    total_users = len(users)
    bar = Bar('Progress', max=total_users/100)
    # import pdb; pdb.set_trace()
    all_models = {}
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
            tuple_model = tuple(site_list)
            if tuple_model in all_models:
                all_models[tuple_model] += 1
            else:
                all_models[tuple_model] = 1

        # if counter >= 50:
            # break
            # import pdb; pdb.set_trace()

    bar.finish()

    elapsed = datetime.now() - start_model
    print
    print "Total models...", len(all_models)
    print "Model total time:", elapsed
    print total_hit, total_miss
    print

    return all_models

def save_page_counts(users_count):
    global cursor, db
    print "Saving path sizes..."
    for path_size, total_users in users_count.items():
        # print path_size, total_users
        cursor.execute(""" insert into path_sizes_new 
            (path_size, total_users)
            values(%s, %s)
            """, [path_size, total_users] )
    db.commit()


def save_models(all_models):
    global cursor, db
    print "Saving models..."
    bar = Bar('Progress', max=len(all_models)/1000)

    db.begin()
    results_models = []
    results_items = []
    model_id = 0
    # import pdb; pdb.set_trace()
    for sites, count_model in all_models.items():
        results_models.append([model_id, len(sites), count_model])
        for sequence_id, site_name in enumerate(sites):
            results_items.append([model_id, sequence_id, site_name])

        if model_id > 0 and model_id % 1000 == 0:
            bar.next()
            cursor.executemany(""" insert into users_model_items 
                (model_id, sequence_id, site_name)
                values(%s, %s, %s)
                """, (results_items) )
            cursor.executemany(""" insert into users_model 
                (model_id, model_size, count_model)
                values(%s, %s, %s)
                """, (results_models) )
            db.commit()
            db.begin()
            results_items = []
            results_models = []

        model_id += 1

    if len(results_models) > 0:
        cursor.executemany(""" insert into users_model_items 
            (model_id, sequence_id, site_name)
            values(%s, %s, %s)
            """, (results_items) )
        cursor.executemany(""" insert into users_model 
            (model_id, model_size, count_model)
            values(%s, %s, %s)
            """, (results_models) )
        db.commit()
    bar.finish()



def main():
    global cursor, db, s
    print "Program start..."

    start = datetime.now()

    db = MySQLdb.connect("localhost","root","","stream" )
    cursor = db.cursor()

    s = solr.SolrConnection(solr_endpoint)

    start = datetime.now()

    users = process_stream()
    users_count, page_count = count_users(users)
    all_models = model_path(users, users_count)
    if save_results:
        save_page_counts(page_count)
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
