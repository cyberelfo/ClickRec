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

import networkx as nx

G=nx.Graph()
G.add_node("users")
G.add_node("pages")

config = ConfigParser.ConfigParser()
config.read("./stream.ini")

path = config.get('main', 'path')
filename = config.get('main', 'filename')


parser = argparse.ArgumentParser()
parser.add_argument("num_users", type=int, 
	help="Number of users / size of the window")
parser.add_argument("max_transactions", nargs='?', default=0, type=int, 
	help="Number of transactions to load, 0 = all")
args = parser.parse_args()

# Global variables

window_size = args.num_users
target_user = 0
max_transactions = args.max_transactions
bit_array = {} # armazenar todos os documentos 
users = pages_users = [0] * window_size
frequents = {}
support = 0.01

def user_visit_document(user_pos, document_id):
	if document_id in bit_array:
 		bit_array[document_id][user_pos] = 1
 		#G.add_edge(user_id,document_id)
 	else:
 		bit_array[document_id] = bitarray([False] * window_size)
 		bit_array[document_id][user_pos] = 1
		#G.add_node(document_id)
		#G.add_edge(user_id,document_id)

def new_page(user_id, document_id):
	bit_array[document_id] = bitarray([False] * window_size)
	bit_array[document_id][target_user] = True
	#G.add_node(document_id)
	#G.add_edge(user_id,document_id)



def adjust_bit_vector(bit_vector, key):
	if bit_vector.count() == 0:
		del bit_array[key]
		return 1
	else:
		bit_vector[target_user] = False
		return 0

def fix_bit_array(document_id):
	count = 0
	updated = False
	for key, bit_vector in bit_array.items():
		if key == document_id:
			bit_vector[target_user] = True
			updated = True
		else:
			count += adjust_bit_vector(bit_vector, key)
	return count, updated

def replace_user(user_id):
	global target_user
	try:
		removed_user = users[target_user]
	except IndexError:
		removed_user = None

 	#G.remove_node(removed_user)
 	users[target_user] = user_id
 	target_user = (target_user + 1) % window_size
 	#print target_user, removed_user, user_id
 	#G.add_node(user_id)
 	return removed_user


def generate_graph(user_id):
	import matplotlib.pyplot as plt
	nx.draw(G)
	plt.savefig("graph_{0}.png".format(user_id))


def slide_window(size, document_id, user_id):

 	if user_id not in users:

		removed_user = replace_user(user_id)	 	

	 	count, updated = fix_bit_array(document_id)
	 	if count > 100:
		 	print "{0} left , entered {1} at {2} removal of {3} pages".format(removed_user, user_id, target_user, count)

		if not updated:
	 		new_page(user_id, document_id)

	user_pos = users.index(user_id)
 	user_visit_document(user_pos, document_id)
	


def check_array():
	dict_not_ok = False
	for key in bit_array.keys():
		if len(bit_array[key]) != len(users):
			print key
			print len(bit_array[key]) , len(users)
			dict_not_ok = True
	return dict_not_ok


def generate_fis(frequent_size, prev_frequents):
	frequents[frequent_size] = []
	print "generate_fis()"

	if frequent_size == 1:
		for doc_id in bit_array.keys():
			if bit_array[doc_id].count() >= support * len(users):
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
						bit_vector = bit_array[item[1]]
					except:
						import pdb; pdb.set_trace()
				else:
					bit_vector = bit_vector & bit_array[item[1]]
			if bit_vector.count() >= support * len(users):
				frequents[frequent_size].append(itemset)

	print frequent_size, frequents[frequent_size]

	if len(frequents[frequent_size]) > 0:
		generate_fis(frequent_size+1, frequents[frequent_size])

def main():

	print "Program start..."

	start_t = start = dt.now()

	f = open(path+filename, 'rb')

	reader = csv.reader(f)

	num_transactions = 0

	for product_id, _type, document_id, provider_id, user_id, timestamp,  in reader:
		if product_id == '1':  # G1
			num_transactions += 1
			if max_transactions > 0 and num_transactions > max_transactions: 
				break
			
			slide_window(window_size, int(document_id), int(user_id))

			if num_transactions % 1000 == 0:
				stop_t = dt.now()
				tempo_execucao = stop_t - start_t
				print num_transactions, "- Tempo de execucao:", \
					tempo_execucao, \
					"Window size:", len(users), "Pages:", len(bit_array)
				if tempo_execucao.seconds > 30:
					import sys
					sys.exit(0)
				start_t = stop_t

	f.close()

	print "Support:", support * len(users)
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
	#import cProfile
	#cProfile.run('main()')
