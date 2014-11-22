#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import argparse

if __name__ == '__main__':

	parser = argparse.ArgumentParser()
	parser.add_argument("path_size", help="Size of the path")
	parser.add_argument("num_users", help="Number of users to select")
	args = parser.parse_args()

	db = MySQLdb.connect("localhost","root","","stream" )
	cursor = db.cursor()

	path_size = args.path_size
	num_users = args.num_users

	sql = """ select user_id
		from user_path_size
		where path_size = %s
		order by rand()
		limit %s;
		""" % (path_size, num_users)

	cursor.execute(sql)
	users = cursor.fetchall()

	for user in users:
		sql = """ select from_unixtime(substr(s.timestamp, 1, 10)) timestamp, d.url
			from stream_g1 s, document d
			where s.user_id = %s
			and s.document_id = d.document_id
			order by s.timestamp;
			""" % (user[0])

		print
		print user[0]
		cursor.execute(sql)
		results = cursor.fetchall()
		for row in results:
			print row[0], "-", row[1]

	db.close()
