#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime as dt
from datetime import timedelta
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

window_size = args.num_users
num_transactions = args.num_transactions
# window_size = 1000

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
        a = users.index(user_id)
        if document_id in dictionary:
            dictionary[document_id][a] = 1
            # print "Update!"
        else:
            dictionary[document_id] = bitarray([False] * (size - 1))
            dictionary[document_id][a] = 1
            # print "New!"

    else:
        users.append(user_id)
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
    # import pdb; pdb.set_trace()

    if user_id in users:
        i = users.index(user_id)
        if document_id in dictionary:
            dictionary[document_id][i] = 1
            # print "Update!"
        else:
            dictionary[document_id] = bitarray([False] * (size))
            dictionary[document_id][i] = 1
            # print "New!"
    else:
        updated = False
        # print "else"
        del users[0]
        users.append(user_id)

        for key in dictionary.keys():
            del dictionary[key][0]
            if key == document_id:
                dictionary[key].extend([True])
                updated = True
            else:
                if dictionary[key].count() == 0:
                    del dictionary[key]
                else:
                    dictionary[key].extend([False])

        if not updated:
            dictionary[document_id] = bitarray([False] * (size - 1))
            dictionary[document_id].extend([True])



if __name__ == '__main__':

    start = dt.now()
    start_t = dt.now()

    f = open(path+filename, 'rb')

    reader = csv.reader(f)

    for row in enumerate(reader):
        if num_transactions > 0 and row[0] > num_transactions: 
            break
        if len(users) < window_size:
            load_window(len(users), row[1][4], row[1][2])
        else:
            slide_window(window_size, row[1][4], row[1][2])
        if row[0] % 1000 == 0:
            stop_t = dt.now()
            tempo_execucao = stop_t - start_t
            print row[0], "- Tempo de execucao:", \
                tempo_execucao, \
                "Window size:", len(users), "Pages:", len(dictionary)
            # import pdb; pdb.set_trace()

            start_t = stop_t

    f.close()

    stop = dt.now()
    tempo_execucao = stop - start 

    # pprint(dictionary)
    # print users

    print "Fim processamento"
    print "Tempo de execucao:", tempo_execucao