#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
from progress.bar import Bar
import timeit
import csv
import time
import datetime
import ConfigParser


config = ConfigParser.ConfigParser()
config.read("./stream.ini")

path = config.get('main', 'path')
filename = config.get('main', 'filename')

def file_len(fname):
	with open(fname) as f:
		for i, l in enumerate(f):
			pass
	return i + 1
	
def load_stream():
	print "Delete table..."
	cursor.execute(""" delete from stream_g1 where filename = %s ;""", [filename] )

	total_lines = file_len(path+filename)

	f = open(path+filename, 'rb')

	reader = csv.reader(f)

	i = 1
	results = []
	doc_user = set()

	print "Loading table..."
	bar = Bar('Progress', max=total_lines)

	for row in reader:
		bar.next()
		# Select only pageviews from product 1 (G1)
		if row[0] == "1":			
			if (row[2], row[4]) in doc_user:
				# Ignore duplicate user+document 
				pass
			else:
				row_datetime = datetime.datetime.fromtimestamp(int(row[5][:10]))
				row.append(row_datetime)
				row.append(filename)
				results.append(row)
				i += 1
				doc_user.add((row[2], row[4]))


		if i % 1000 == 0:
			cursor.executemany(""" insert into stream_g1
				(product_id, type, document_id, provider_id, user_id, 
					timestamp, stream_datetime, filename)
				values(%s, %s, %s, %s, %s, %s, %s, %s ) ;
				""" , (results))
			db.commit()
			results = []

	if len(results) > 0:
		cursor.executemany(""" insert into stream_g1
			(product_id, type, document_id, provider_id, user_id, 
				timestamp, stream_datetime, filename)
			values(%s, %s, %s, %s, %s, %s, %s, %s ) ;
			""" , (results))
		db.commit()

	f.close()
	bar.finish()

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
