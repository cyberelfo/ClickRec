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

	print "Delete table..."
	cursor.execute(""" delete from count_doc_hits where filename = %s;""", [filename] )

	print "Populate table..."
	cursor.execute(""" insert into count_doc_hits (document_id, filename, count)
		select document_id, filename, count(*) from stream_g1
		where filename = %s
		group by document_id;
		""", [filename] )

	print "Update table..."
	cursor.execute(""" update count_doc_hits c
		join document d on c.document_id = d.document_id
			and c.filename = d.filename
		set c.url = d.url
		where c.filename = %s;
		""", [filename] )

	db.commit()

	cursor.close()
	db.close()