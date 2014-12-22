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

def load_window(size, document_id, user_id):
 	if user_id in users:
 		user_pos = users.index(user_id)
 		if document_id in dictionary:
	 		dictionary[document_id][user_pos] = 1
	 	else:
	 		dictionary[document_id] = bitarray([False] * (len(users)))
	 		dictionary[document_id][user_pos] = 1
 	else:
		updated = False
 		users.append(user_id)
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
	 	else:
	 		dictionary[document_id] = bitarray([False] * (len(users)))
	 		dictionary[document_id][user_pos] = 1
	else:
		updated = False
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


def generate_fis(frequent_size, prev_frequents):
	frequents[frequent_size] = []
	print "generate_fis()"

	if frequent_size == 1:
		for doc_id in dictionary.keys():
			if dictionary[doc_id].count() >= support * len(users):
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
					try:
						bitarray = dictionary[item[1]]
					except:
						import pdb; pdb.set_trace()
				else:
					bitarray = bitarray & dictionary[item[1]]
			if bitarray.count() >= support * len(users):
				frequents[frequent_size].append(itemset)

	print frequent_size, frequents[frequent_size]

	if len(frequents[frequent_size]) > 0:
		generate_fis(frequent_size+1, frequents[frequent_size])


if __name__ == '__main__':

	print "Program start..."

	start = timeit.default_timer()
	start_t = timeit.default_timer()

	f = open(path+filename, 'rb')

	reader = csv.reader(f)

	num_transactions = 0
	for row in enumerate(reader):
		if row[1][0] == '1':
			num_transactions += 1
			if max_transactions > 0 and num_transactions > max_transactions: 
				break
			if len(users) < window_size:
				load_window(num_transactions, int(row[1][2]), int(row[1][4]))
			else:
				slide_window(window_size, int(row[1][2]), int(row[1][4]))
			# if check_array():
			# 	pprint(dictionary)
			# 	break

			if num_transactions % 1000 == 0:
				stop_t = timeit.default_timer()
				tempo_execucao = stop_t - start_t
				print num_transactions, "- Tempo de execucao:", \
					time.strftime('%Hhs %Mmin %Sseg', time.gmtime(tempo_execucao)), \
					"Window size:", len(users), "Pages:", len(dictionary)
				start_t = stop_t

	f.close()

	print "Support:", support * len(users)
	generate_fis(1, [])

	stop = timeit.default_timer()
	tempo_execucao = stop - start 

	# print "dictionary"
	# pprint(dictionary)
	# print users

	print "Fim processamento"
	print "Tempo de execucao:", time.strftime('%Hhs %Mmin %Sseg', time.gmtime(tempo_execucao))
