#!/usr/bin/env python
# -*- coding: utf-8 -*-

import timeit
import csv
import time
from itertools import combinations
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
frequents = {}

support = 0.01

parser = argparse.ArgumentParser()
parser.add_argument("num_users", type=int, 
	help="Number of users / size of the window")
parser.add_argument("max_transactions", nargs='?', default=0, type=int, 
	help="Number of transactions to load, 0 = all")
args = parser.parse_args()

window_size = args.num_users
max_transactions = args.max_transactions

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
 	if user_id in users:
 		user_pos = users.index(user_id)
 		if document_id in dictionary:
	 		dictionary[document_id][user_pos] = 1
	 		# print "Update!"
	 	else:
	 		dictionary[document_id] = bitarray([False] * (len(users)))
	 		dictionary[document_id][user_pos] = 1
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
	 		dictionary[document_id] = bitarray([False] * (len(users) - 1))
	 		dictionary[document_id].extend([True])

def slide_window(size, document_id, user_id):
 	if user_id in users:
 		user_pos = users.index(user_id)
 		if document_id in dictionary:
	 		dictionary[document_id][user_pos] = 1
	 		# print "Update!"
	 	else:
	 		dictionary[document_id] = bitarray([False] * (len(users)))
	 		dictionary[document_id][user_pos] = 1
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
	 		dictionary[document_id] = bitarray([False] * (len(users) - 1))
	 		dictionary[document_id].extend([True])

def check_array():
	dict_not_ok = False
	for key in dictionary.keys():
		if len(dictionary[key]) <> len(users):
			print key
			print len(dictionary[key]) , len(users)
			dict_not_ok = True
	return dict_not_ok


def generate_fis(frequent_size):
	frequents[frequent_size] = []
	print "generate_fis()"
	for doc_id in dictionary.keys():
		# print dictionary[doc_id]
		# print doc_id, dictionary[doc_id].count()
		if dictionary[doc_id].count() >= support * window_size:
			frequents[frequent_size].append(doc_id)

	print frequents

	item_combinations = list(combinations(frequents[frequent_size], frequent_size + 1))
	for itemset in item_combinations:
		for item in enumerate(itemset):
			# print dictionary[item[1]]
			if item[0] == 0:
				bitarray = dictionary[item[1]]
			else:
				bitarray = bitarray & dictionary[item[1]]

		if bitarray.count() >= support * window_size:
			print itemset
			print bitarray



if __name__ == '__main__':

	print "Program start..."

	start = timeit.default_timer()
	start_t = timeit.default_timer()

	f = open(path+filename, 'rb')

	reader = csv.reader(f)

	num_transactions = 0
	print "Support:", support * window_size
	for row in enumerate(reader):
		# print row
		if row[1][0] == '1':
			num_transactions += 1
			if max_transactions > 0 and num_transactions > max_transactions: 
				break
			if len(users) < window_size:
				load_window(num_transactions, row[1][2], row[1][4])
			else:
				slide_window(window_size, row[1][2], row[1][4])
			# if check_array():
			# 	pprint(dictionary)
			# 	break

			if num_transactions % 1000 == 0:
				stop_t = timeit.default_timer()
				tempo_execucao = stop_t - start_t
				print num_transactions, "- Tempo de execucao:", \
					time.strftime('%Hhs %Mmin %Sseg', time.gmtime(tempo_execucao)), \
					"Window size:", len(users), "Pages:", len(dictionary)
				# print dictionary[row[1][4]]
				start_t = stop_t

			# if num_transactions % window_size == 0:
			# 	generate_fis(1)			

	f.close()

	generate_fis(1)

	stop = timeit.default_timer()
	tempo_execucao = stop - start 

	# print "dictionary"
	# pprint(dictionary)
	# print users

	print "Fim processamento"
	print "Tempo de execucao:", time.strftime('%Hhs %Mmin %Sseg', time.gmtime(tempo_execucao))
