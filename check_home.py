#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import ConfigParser
import datetime

config = ConfigParser.ConfigParser()
config.read("./stream.ini")

filename = config.get('main', 'filename')

if __name__ == '__main__':

	year = int(filename[16:20])
	month = int(filename[21:23])
	day = int(filename[24:26])
	hour = int(filename[27:29])
	min = 0

	# rt-actions-read-2014_11_29_18.log

	dt_file_fim = datetime.datetime(year, month, day, hour, 00)
	dt_file_inicio = dt_file_fim - datetime.timedelta(hours=1)

	db = MySQLdb.connect("localhost","root","","stream" )
	cursor = db.cursor()

	cursor.execute(""" 
		select distinct d.url 
		from itemset i , document d,  home_g1 h
		where i.itemset_size = 1
		and i.document_id = d.document_id
		and d.url_md5 = h.url_md5
		and h.datetime_crawl >= %s
		and h.datetime_crawl <= %s
		and i.filename = %s ;
		""" , [dt_file_inicio, dt_file_fim, filename])

	result = cursor.fetchall()

	for row in enumerate(result):
		print row[0], row[1][0]

	db.close()
