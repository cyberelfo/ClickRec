#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb

if __name__ == '__main__':


	db = MySQLdb.connect("localhost","root","","stream" )
	cursor = db.cursor()

	print "Truncate tables..."
	cursor.execute(""" truncate table path_sizes;""" )
	cursor.execute(""" truncate table user_path_size;""" )

	print "Populate tables..."
	cursor.execute(""" insert into user_path_size
		select user_id, count(*) path_size
		from stream_g1
		group by user_id;
		""" )

	cursor.execute(""" insert into path_sizes (path_size, num_users)
		select path_size, count(*) num_users
		from user_path_size
		group by path_size;
		""" )

	print "Update table..."
	cursor.execute(""" select sum(num_users) from path_sizes;""" )
	result = cursor.fetchone()
	
	cursor.execute(""" update path_sizes
		set percent = round(num_users / %s, 2)
	""", [result[0]] )
	
	db.commit()

	cursor.close()
	db.close()