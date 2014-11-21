#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from BeautifulSoup import BeautifulSoup
import MySQLdb

url = "http://g1.globo.com"
response = requests.get(url)
# parse html
page = str(BeautifulSoup(response.content))


def getURL(page):
	"""

	:param page: html of web page (here: Python home page) 
	:return: urls in that page 
	"""
	start_link = page.find("a href")
	if start_link == -1:
		return None, 0
	start_quote = page.find('"', start_link)
	end_quote = page.find('"', start_quote + 1)
	url = page[start_quote + 1: end_quote]
	return url, end_quote


db = MySQLdb.connect("localhost","root","","stream" )
cursor = db.cursor()
cursor.execute(""" truncate table home_g1;""" )

while True:
	url, n = getURL(page)
	page = page[n:]
	if url:
		print url
		# print type(url)
		# print (""" insert into home_g1(url) values(%s) ;""" , (url) )
		# cursor.execute(""" select 1; """)
		cursor.execute(""" insert into home_g1(url) values(%s) ;""" , [url] )
	else:
		break

db.commit()