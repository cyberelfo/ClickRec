#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb
import timeit
from itertools import combinations
import sys
import time
from progress.bar import Bar

# transactions = {}
frequent_size = {}
support = 0.02
# num_linhas = 3000
num_linhas = 10000000
nome_tabela = "stream_new"
# nome_tabela = "stream_new_valida"
produto = 3

# transactions_list = []

def to_transaction(linhas):
	_transactions = {}
	_transactions_list = []
	for linha in linhas:
		userid = linha[5]
		document = linha[3]

		if userid in _transactions:
			_transactions[userid].append(document)
		else:
			_transactions[userid] = [document]

	print ""
	print "Stream to transaction..."
	bar = Bar('Progress', max=len(_transactions))
	commit_count = 0
	# cursor.execute("""truncate table transaction;""")

	for transaction in _transactions.itervalues():
		_transactions_list.append(transaction)
		# cursor.execute("""INSERT INTO transaction(user_id, document_id) VALUES (%s,%s)""",(userid,str(transaction)))
		# if commit_count % 10000 == 0:
		# 	db.commit()
		bar.next()
	# db.commit()
	bar.finish()

	# import pdb; pdb.set_trace()

	return _transactions_list


def delete_transactions(transactions, size):
	old = 0
	new = 0
	new_transactions = []
	for transaction in transactions:
		old += 1
		if len(transaction) > size:
			new_transactions.append(set(transaction))
			new += 1

	print ""
	print "Before/after delete:", old, new
	return new_transactions

def count_items(transactions_list):
	items = set()
	count = {}
	frequent = []
	for transaction in transactions_list:
		for item in transaction:
			if item in items:
				count[item] += 1
			else:
				items.add(item)
				count[item] = 1

	_transactions_count = len(transactions_list)

	print "Items:", len(items), "Transactions:", _transactions_count, "Support:", _transactions_count * support

	return items, count

def frequent_items(transactions, items, count):
	frequent_size[1] = []
	frequent = set()
	i = 0

	print (len(transactions) * support)
	for item in items:
		if count[item] >= (len(transactions) * support):
			frequent_size[1].append(item)
			print "item/count:", item, count[item]

	print ""
	print "Frequent size 1:", len(frequent_size[1])
	print frequent_size[1]
	print "--------------"


def frequent_itemsets(transactions_list, previous_frequent, size):
	print ""
	print "Size:", size

	frequent = set()
	frequent_candidates_clean = []
	current_support = len(transactions_list) * support
	frequent_size[size] = []

	c = list(combinations(previous_frequent, size))

	print ""
	print "previous_frequent:", len(previous_frequent)
	print "Generating candidates size " + str(size) + "..."
	bar = Bar('Progress', max=len(c))

	if size == 2:
		for i in c:
			frequent_candidates_clean.append(set(i))
			bar.next()
	else:
		for i in c:
			for f in frequent_size[size - 1]:
				if f.issubset(i) and set(i) not in frequent_candidates_clean:
					frequent_candidates_clean.append(set(i))
			bar.next()
	bar.finish()

	print "frequent_candidates_clean:", len(frequent_candidates_clean)

	# import pdb; pdb.set_trace()

	print ""
	print "Checking candidates size " + str(size) + "..."
	bar = Bar('Progress', max=len(frequent_candidates_clean))

	for itemset in frequent_candidates_clean:
		# workset = set(itemset)
		count = 0
		for transaction in transactions_list:
			if transaction.issuperset(itemset):				
				count += 1
		# print workset, "count: ", count
		if count >= current_support:
			frequent.update(itemset)
			frequent_size[size].append(set(itemset))
			# print "Frequent item size " + str(size) + ":", itemset
		bar.next()
	bar.finish()

	print ""
	# print "Frequent:", frequent
	print "Frequent size " + str(size) + ":", len(frequent_size[size])
	print frequent_size[size]
	# print frequent
	print "--------------"

	transactions_list = delete_transactions(transactions_list, size)

	if frequent == set([]):
		return None

	frequent_itemsets(transactions_list, frequent, size + 1)

def validate_count(transactions_list, list_count):
	total_transaction = 0
	for i in transactions_list:
		total_transaction += len(i)

	total_count = 0
	for i in list_count.itervalues():
		total_count += i

	print "Total linhas:", total_linhas
	print "Total itens transaction:", total_transaction
	print "Total count:", total_count

if __name__ == '__main__':

	transactions_list = []

	start = timeit.default_timer()
	
	db = MySQLdb.connect("localhost","root","","stream" )
	cursor = db.cursor()
	sql = """ select id, product_id, type, document_id, 
						provider_id, user_id, timestamp 
						from %s 
						where product_id = %s
						limit %s """ % (nome_tabela, produto, num_linhas)

	print "Executing query..."
	cursor.execute(sql)
	result = cursor.fetchall()
	# checkpoint = timeit.default_timer()
	# print "Checkpoint:", checkpoint - start 

	# import pdb; pdb.set_trace()

	total_linhas = len(result)

	print "Converting stream to transactions..."
	transactions_list = to_transaction(result)
	# checkpoint = timeit.default_timer()
	# print "Checkpoint:", checkpoint - start 

	print "Counting items..."
	list_items, list_count = count_items(transactions_list)
	# checkpoint = timeit.default_timer()
	# print "Checkpoint:", checkpoint - start 

	print "Validate count..."
	validate_count(transactions_list, list_count)

	print "Selecting frequent items..."
	frequent_items(transactions_list, list_items, list_count)
	# checkpoint = timeit.default_timer()
	# print "Checkpoint:", checkpoint - start 

	transactions_list = delete_transactions(transactions_list, 1)
	# checkpoint = timeit.default_timer()
	# print "Checkpoint:", checkpoint - start 

	print "Selecting frequent itemsets..."
	frequent_itemsets(transactions_list, frequent_size[1], 2)

	stop = timeit.default_timer()
	tempo_execucao = stop - start 

	print "Fim processamento"
	print "Tempo de execucao (segundos):", tempo_execucao

	db.close()
