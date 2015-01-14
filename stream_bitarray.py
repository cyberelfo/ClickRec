#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import time
from itertools import combinations
from pprint import pprint
from bitarray import bitarray
import argparse
import ConfigParser
from datetime import datetime as dt
import requests

config = ConfigParser.ConfigParser()
config.read("./stream.ini")

path = config.get('main', 'path')
filename = config.get('main', 'filename')
local_file = config.getboolean('main', 'local_file')
support = config.getfloat('main', 'support')

hadoop_server = config.get('hadoop', 'hadoop_server')
hadoop_port = config.get('hadoop', 'hadoop_port')
hadoop_path = config.get('hadoop', 'hadoop_path')

parser = argparse.ArgumentParser()

parser.add_argument("num_users", type=int, 
	help="Number of users / size of the window")
parser.add_argument("max_transactions", nargs='?', default=0, type=int, 
	help="Number of transactions to load, 0 = all")
args = parser.parse_args()

# Global variables

window_size = args.num_users
window_not_full = True
target_user = 0
max_transactions = args.max_transactions
bit_array = {} # armazenar os bit_vectors de todos os documentos
users = [0] * window_size
users_dict = {}
pages_users = [set() for i in range(window_size)]
frequents = {}


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
	# s.write(r.content)
	# f = s.getvalue()
	return s

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

def user_visit_document(user_pos, document_id):
	pages_users[user_pos].add(document_id)
	try:
 		bit_array[document_id][user_pos] = True
 		#G.add_edge(user_id,document_id)
 	except KeyError:
 		bit_array[document_id] = bitarray([False] * window_size)
 		bit_array[document_id][user_pos] = True
		#G.add_node(document_id)
		#G.add_edge(user_id,document_id)

def replace_user(user_id):
	removed_user = users[target_user]
	try:
		del users_dict[removed_user]
	except KeyError:
		removed_user = None
	for doc_id in pages_users[target_user]:
		bit_array[doc_id][target_user] = False
		if bit_array[doc_id].count() == 0:
			del bit_array[doc_id]

 	#G.remove_node(removed_user)
 	users[target_user] = user_id
 	pages_users[target_user] = set()
 	users_dict[user_id] = target_user
 	#print target_user, removed_user, user_id
 	#G.add_node(user_id)
 	return removed_user

def slide_window(size, document_id, user_id):
	global target_user, window_not_full
 	if user_id not in users_dict:

		removed_user = replace_user(user_id)
	 	target_user = (target_user + 1) % window_size
	 	if window_not_full and target_user == 0:
	 		window_not_full = False

	user_pos = users_dict[user_id]
 	user_visit_document(user_pos, document_id)
	
def generate_fis(frequent_size, prev_frequents):
	frequents[frequent_size] = []
	# print "generate_fis()"

	if window_not_full:
		cur_window_size = target_user
	else:
		cur_window_size = window_size

	if frequent_size == 1:
		# print "Support:", support * cur_window_size
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

	if frequent_size > 1 and len(frequents[frequent_size]) > 0:
		print "Support:", support * cur_window_size
		print frequent_size, frequents[frequent_size]

	if len(frequents[frequent_size]) > 0:
		generate_fis(frequent_size+1, frequents[frequent_size])

def debug_array(user_id, document_id, target_user):
	print ""
	print "User:", user_id, "[", target_user,"]"
	print "Document:", document_id
	pprint(bit_array)

def main():

	print "Program start..."

	start_t = start = dt.now()

	print "Reading stream file..."

	if local_file:
		f = open(path+filename, 'rb')
	else:
		f = read_from_hadoop("rt-actions-read-2015_01_14_12.log")

	stream = csv.reader(f)

	print "Sorting stream..."

	stream_sorted = sort_by_column(stream, 5)

	stop = dt.now()
	tempo_execucao = stop - start 

	print "Stream sorted in:", tempo_execucao 

	num_transactions = 0
	cur_timestamp = 0
	for product_id, _type, document_id, provider_id, user_id, timestamp  in stream_sorted:
		if product_id == '1':  # G1
			num_transactions += 1
			if max_transactions > 0 and num_transactions > max_transactions: 
				break
			
			slide_window(window_size, int(document_id), int(user_id))

			# if num_transactions == 50:
			# 	import pdb; pdb.set_trace()
			if num_transactions % 1000 == 0:
				stop_t = dt.now()
				tempo_execucao = stop_t - start_t
				row_datetime = dt.fromtimestamp(int(timestamp[:10]))
				print num_transactions, "- Execution time:", \
					tempo_execucao, \
					"Array pointer:", target_user, "Pages:", \
					len(bit_array), "Row timestamp:", row_datetime
				start_t = stop_t

			if num_transactions % 10000 == 0:
				generate_fis(1, [])

			# debug_array(user_id, document_id, target_user)

	f.close()

	# print timestamp
	# print dt.fromtimestamp(int(timestamp[:10]))
	generate_fis(1, [])

	stop = dt.now()
	tempo_execucao = stop - start 

	# print "bit_array"
	# pprint(bit_array)
	# print users

	print "Fim processamento"
	print "Tempo de execucao:", tempo_execucao

if __name__ == '__main__':
	main()
	# import cProfile
	# cProfile.run('main()')
