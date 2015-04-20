#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import stream_bitarray_recommend as sbr
import redis

user_path_size = 2
filename = 'rt-actions-read-2015_01_14_00.log'
product_id = 2

def main():

	print "Program start..."

	r = redis.StrictRedis(host='localhost', port=6379, db=0)

	topnews = [i[8:] for i in r.zrevrange('DOC_COUNTS', 0, 9)]

	db = MySQLdb.connect("localhost","root","","stream" )
	cursor = db.cursor()


	sql = """ select user_id, count(*) 
			from stream 
			where product_id = %s
			and filename = '%s'
			group by user_id
			having count(*) >= %s
			limit 200;
		""" % (product_id, filename, user_path_size)

	print "Selecting users"

	cursor.execute(sql)

	print "Fetching users"
	users = cursor.fetchall()

	print "Recommending and checking..."

	hit = 0
	hit_topnews = 0
	count = 0
	for user in users:
		count += 1

		sql = """ select document_id 
				from stream 
				where product_id = 2
				and filename = 'rt-actions-read-2015_01_14_00.log'
				and user_id = %s
				order by stream_datetime;
			""" % (user[0])

		cursor.execute(sql)
		documents = cursor.fetchall()

		documents = [str(i[0]) for i in documents]

		head = documents[0]
		tail = set(documents[1:])

		result = sbr.calc(head, 1)

		intersect = set(result) & tail

		if len(intersect) > 0:
			hit += 1

		intersect = set(topnews) & tail

		if len(intersect) > 0:
			hit_topnews += 1

	print
	print "Result"
	print "Total users:", count, "Hits:", hit, "Hits TopTen:", hit_topnews

if __name__ == '__main__':
    main()
