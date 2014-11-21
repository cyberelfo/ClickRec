#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb

if __name__ == '__main__':


	db = MySQLdb.connect("localhost","root","","stream" )
	cursor = db.cursor()

	print "Truncate table..."
	cursor.execute(""" truncate table count_doc_hits;""" )

	print "Populate table..."
	cursor.execute(""" insert into count_doc_hits (document_id, count)
		select document_id, count(*) from stream_g1
		group by document_id;
		""" )

	print "Update table..."
	cursor.execute(""" update count_doc_hits c
		set c.url = (select d.url from document d
		where c.document_id = d.document_id);
		""" )


	cursor.execute(""" update count_doc_hits c
		set c.url = (select d.url from document d
		where c.document_id = d.document_id);
		""" )

	db.commit()

	cursor.close()
	db.close()