#!/usr/bin/env python
# -*- coding: utf-8 -*-

import timeit
import csv
import time
from pprint import pprint
from bitarray import bitarray
import argparse
import ConfigParser

config = ConfigParser.ConfigParser()
config.read("./stream.ini")

path = config.get('main', 'path')
filename = config.get('main', 'filename')

dictionary = {} # armazenar todos os documentos 
users = []

parser = argparse.ArgumentParser()
parser.add_argument("num_users", type=int, 
    help="Number of users / size of the window")
parser.add_argument("num_transactions", nargs='?', default=0, type=int, 
    help="Number of transactions to load, 0 = all")
args = parser.parse_args()

target_user = 0
window_size = args.num_users
num_transactions = args.num_transactions
pages_users = [set() for i in range(window_size)]

def populate_array(size, user_id, document_id):

    update_documents(size, document_id)


def fast_update_documents(size, document_id, dictionary):
    dictionary = dict(map((lambda (k,x): (k, x + bitarray(1))), dictionary.iteritems()))

    try:
        dictionary[document_id].extend([True])
    except KeyError:
        dictionary[document_id] = bitarray([False] * (size - 1))
        dictionary[document_id].extend([True])

    return dictionary

def test_update_documents(size, document_id):
    size +=1 
    updated = False
    for key in dictionary:
        if key == document_id:
            dictionary[key] = 1
            updated = True
        else:
            dictionary[key] += 1

    if not updated:
        dictionary[document_id] = size

def load_window(size, document_id, user_id):
    size +=1 

    if user_id in users:
        i = users.index(user_id)
        pages_users[i].add(document_id)

        if document_id in dictionary:
            dictionary[document_id][i] = 1
            # print "Update!"
        else:
            dictionary[document_id] = bitarray([False] * (size))
            dictionary[document_id][i] = 1
            # print "New!"

    else:
        users.append(user_id)
        pages_users[size - 1].add(document_id)

        updated = False
        for key in dictionary:
            if key == document_id:
                dictionary[key].extend([True])
                updated = True
            else:
                dictionary[key].extend([False])

        if not updated:
            dictionary[document_id] = bitarray([False] * (size - 1))
            dictionary[document_id].extend([True])

def slide_window(size, document_id, user_id):
    global target_user

    if user_id in users:
        i = users.index(user_id)
        pages_users[i].add(document_id)
        if document_id in dictionary:
            dictionary[document_id][i] = 1
            # print "Update!"
        else:
            dictionary[document_id] = bitarray([False] * (size))
            dictionary[document_id][i] = 1
            # print "New!"
    else:
        # print "else"
        users[target_user] = user_id

        for doc_id in pages_users[target_user]:
            dictionary[doc_id][target_user] = 0
            if dictionary[doc_id].count() == 0:
                del dictionary[doc_id]

        pages_users[target_user] = set([document_id])

        if document_id in dictionary:
            dictionary[document_id][target_user] = 1
            # print "Update!"
        else:
            dictionary[document_id] = bitarray([False] * (size))
            dictionary[document_id][target_user] = 1
            # print "New!"

        target_user = (target_user + 1) % window_size


if __name__ == '__main__':

    start = timeit.default_timer()
    start_t = timeit.default_timer()

    f = open(path+filename, 'rb')

    reader = csv.reader(f)

    for row in enumerate(reader):
        # print row
        # import pdb; pdb.set_trace()
        if num_transactions > 0 and row[0] > num_transactions: 
            break
        if len(users) < window_size:
            load_window(len(users), row[1][4], row[1][2])
        else:
            slide_window(row[0], row[1][4], row[1][2])
        if row[0] % 1000 == 0:
            stop_t = timeit.default_timer()
            tempo_execucao = stop_t - start_t
            print row[0], "- Tempo de execucao:", \
                time.strftime('%Hhs %Mmin %Sseg', time.gmtime(tempo_execucao)), \
                "Window size:", len(users), "Pages:", len(dictionary)
            start_t = stop_t

    f.close()

    stop = timeit.default_timer()
    tempo_execucao = stop - start 

    # pprint(dictionary)
    # print users

    print "Fim processamento"
    print "Tempo de execucao:", time.strftime('%Hhs %Mmin %Sseg', time.gmtime(tempo_execucao))