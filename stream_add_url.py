#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import solr
import timeit
import time
from progress.bar import Bar
import datetime
import ConfigParser

config = ConfigParser.ConfigParser()
config.read("./stream.ini")

solr_endpoint = config.get('main', 'solr_endpoint')
mysql_g1_endpoint = config.get('main', 'mysql_g1_endpoint')
mysql_user = config.get('main', 'mysql_user')
mysql_password = config.get('main', 'mysql_password')
filename = config.get('main', 'filename')

start = timeit.default_timer()

s = solr.SolrConnection(solr_endpoint)

db_g1 = MySQLdb.connect(mysql_g1_endpoint,mysql_user,mysql_password,"g1", charset='utf8' )
cursor_g1 = db_g1.cursor()

db = MySQLdb.connect("localhost","root","","stream", charset='utf8' )
cursor = db.cursor()

print "Delete table..."
cursor.execute(""" delete from document where filename = %s;""", [filename] )

print "Populate table..."
total_documents = cursor.execute(""" insert into document(document_id, filename) 
					select distinct document_id, filename 
					from stream_g1 where filename = %s;""" , [filename])

print "Total:", total_documents

print "Executing query..."
cursor.execute(""" select document_id
					from document
					where 
					filename = %s ;""", [filename] )

# import pdb; pdb.set_trace()

bar = Bar('Progress', max=total_documents)

i = 0
for result in cursor:
	bar.next()
	response = s.query('documentId:'+str(result[0]), fl="url")
	if response.numFound == 1:
		url = response.results[0]['url']
		title = response.results[0]['title']
		publish_date = response.results[0]['issued']
		modify_date = response.results[0]['modified']
		section = response.results[0]['section'][0]

		# import pdb; pdb.set_trace()

		# body = response.results[0]['body']
		body = ""
		publisher = response.results[0]['publisher']
		# print result[0], "-", url

		if publisher == "G1" :
			permalink = url[len("http://g1.globo.com"):]
			sql_g1 = """select corpo 
							from materia 
							where permalink = %s """
			# print ""
			# print permalink
			# print (sql_g1, (permalink))
			cursor_g1.execute(sql_g1, ([permalink]))
			result_g1 = cursor_g1.fetchone()
			if result_g1 is not None:
				body = result_g1[0]
			else:
				body = ""

		sql = """update document
							set url = %s,
								title = %s,
								body = %s,
								publish_date = %s,
								modify_date = %s,
								section = %s,
								url_md5 = md5(%s)
							where document_id = %s
							and filename = %s """
		# import pdb; pdb.set_trace()

		cursor.execute(sql, (url, title, body, publish_date.replace(tzinfo=None), modify_date.replace(tzinfo=None), section,url, str(result[0]), filename))
		i += 1
		if i % 100 == 0:
			db.commit()
	# else:
		# print result[0], "- none"

bar.finish()

db.commit()


cursor_g1.close()
db_g1.close()

cursor.close()
db.close()


stop = timeit.default_timer()
tempo_execucao = stop - start 

print "Fim processamento"
print "Tempo de execucao:", time.strftime('%Hhs %Mmin %Sseg', time.gmtime(tempo_execucao))
