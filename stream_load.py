#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
from progress.bar import Bar
import timeit
import csv

path = '/Users/franklin/Downloads/'
filename = 'rt-actions-read-2014_11_21_16.log'

def load_stream():
	print "Truncate table..."
	cursor.execute(""" truncate table stream;""" )

	f = open(path+filename, 'rb')

	reader = csv.reader(f)

	i = 1
	results = []
	for row in reader:
		# print row
		if row[0] == "1":
			results.append(row)
			i += 1

		if i % 1000 == 0:
			cursor.executemany(""" insert into stream
				(product_id, type, document_id, provider_id, user_id, timestamp)
				values(%s, %s, %s, %s, %s, %s ) ;
				""" , (results))
			db.commit()
			results = []
			print i

	db.commit()

	f.close()

def remove_dup():

	print "Truncate table..."
	cursor.execute(""" truncate table stream_g1;""" )

	print "Populate table..."
	total_result = cursor.execute("""
			select product_id, type, document_id, provider_id, user_id, timestamp 
			from stream
			where product_id = 1 
			order by user_id, document_id, timestamp ; 
		""" )

	user_id_prev = 0
	document_id_prev = 0

	bar = Bar('Inserting', max=total_result)
	i = 0
	results = []
	for row in cursor:
		if user_id_prev <> row[4] or document_id_prev <> row[2]:
			results.append(row)
			i += 1
			teste = "Append"
			user_id_prev = row[4] 
			document_id_prev = row[2]
		else:
			teste = "Igual"

		# print row[4], row[2], "**", user_id_prev, document_id_prev, teste

		if i % 1000 == 0:
			cursor.executemany(""" insert into stream_g1
				(product_id, type, document_id, provider_id, user_id, timestamp)
				values(%s, %s, %s, %s, %s, %s ) ;
				""" , (results) )

			db.commit()
			results = []

		bar.next()

	cursor.executemany(""" insert into stream_g1
		(product_id, type, document_id, provider_id, user_id, timestamp)
		values(%s, %s, %s, %s, %s, %s ) ;
		""" , (results) )
	# print "deleted", deleted

	db.commit()

	bar.finish()

	cursor.close()
	db.close()

if __name__ == '__main__':

	start = timeit.default_timer()

	db = MySQLdb.connect("localhost","root","","stream" )
	cursor = db.cursor()

	cursor.execute(""" SET autocommit=0; 
		""" )

	load_stream()
	remove_dup()

	stop = timeit.default_timer()
	tempo_execucao = stop - start 

	print "Fim processamento"
	print "Tempo de execucao:", time.strftime('%Hhs %Mmin %Sseg', time.gmtime(tempo_execucao))
