#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import ConfigParser


config = ConfigParser.ConfigParser()
config.read("./stream.ini")

filename = config.get('main', 'filename')

if __name__ == '__main__':


	db = MySQLdb.connect("localhost","root","","stream" )
	cursor = db.cursor()

	print "Delete tables..."
	cursor.execute(""" delete from path_sizes where filename = %s ;""", [filename] )
	cursor.execute(""" delete from user_path_size where filename = %s ;""", [filename] )

	print "Populate tables..."
	cursor.execute(""" insert into user_path_size
		select user_id, count(*) path_size, filename
		from stream_g1
		where filename = %s
		group by user_id;
		""", [filename] )

	cursor.execute(""" insert into path_sizes (path_size, num_users, filename)
		select path_size, count(*) num_users, filename
		from user_path_size
		where filename = %s
		group by path_size;
		""", [filename] )

	print "Update table..."
	cursor.execute(""" select sum(num_users) 
		from path_sizes where filename = %s;""", [filename] )
	result = cursor.fetchone()
	
	cursor.execute(""" update path_sizes
		set percent = round(num_users / %s, 2)
		where filename = %s;
	""", [result[0], filename] )
	
	db.commit()

	cursor.execute(""" select path_size, num_users, percent 
		from path_sizes
		where filename = %s
		order by path_size;
		""", [filename] )

	results = cursor.fetchall()
	print
	print "Path Size - Total users - Percent"
	for row in results:
		print "[" +str(row[0])+ "] -", row[1], "-", str(row[2] * 100) + "%"

	cursor.close()
	db.close()