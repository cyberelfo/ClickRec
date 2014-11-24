#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
from progress.bar import Bar
import timeit
import csv
import time

path = '/Users/franklin/Downloads/'
filename = 'rt-actions-read-2014_11_21_16.log'

def load_stream():
	print "Truncate table..."
	cursor.execute(""" truncate table stream_g1;""" )

	f = open(path+filename, 'rb')

	reader = csv.reader(f)

	i = 1
	results = []
	doc_user = set()
	for row in reader:
		# print row
		if row[0] == "1":
			if (row[2], row[4]) in doc_user:
				pass
			else:
				results.append(row)
				i += 1
				doc_user.add((row[2], row[4]))


		if i % 1000 == 0:
			cursor.executemany(""" insert into stream_g1
				(product_id, type, document_id, provider_id, user_id, timestamp)
				values(%s, %s, %s, %s, %s, %s ) ;
				""" , (results))
			db.commit()
			results = []
			print i

	if len(results) > 0:
		cursor.executemany(""" insert into stream
			(product_id, type, document_id, provider_id, user_id, timestamp)
			values(%s, %s, %s, %s, %s, %s ) ;
			""" , (results))
		db.commit()
		print i

	f.close()

if __name__ == '__main__':

	start = timeit.default_timer()

	db = MySQLdb.connect("localhost","root","","stream" )
	cursor = db.cursor()

	cursor.execute(""" SET autocommit=0; """ )

	load_stream()

	cursor.execute(""" SET autocommit=1; """ )

	stop = timeit.default_timer()
	tempo_execucao = stop - start 

	print "Fim processamento"
	print "Tempo de execucao:", time.strftime('%Hhs %Mmin %Sseg', time.gmtime(tempo_execucao))
