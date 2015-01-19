#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from BeautifulSoup import BeautifulSoup
import MySQLdb
from datetime import datetime
import sys
import time
from PyQt4.QtCore import *
from PyQt4.QtGui import *
from PyQt4.QtWebKit import *


class Screenshot(QWebView):
	def __init__(self):
		self.app = QApplication(sys.argv)
		QWebView.__init__(self)
		self._loaded = False
		self.loadFinished.connect(self._loadFinished)

	def capture(self, url, output_file):
		self.load(QUrl(url))
		self.wait_load()
		# set to webpage size
		frame = self.page().mainFrame()
		self.page().setViewportSize(frame.contentsSize())
		# render image
		image = QImage(self.page().viewportSize(), QImage.Format_ARGB32)
		painter = QPainter(image)
		frame.render(painter)
		painter.end()
		print 'saving', output_file
		image.save(output_file)

	def wait_load(self, delay=0):
		# process app events until page loaded
		while not self._loaded:
			self.app.processEvents()
			time.sleep(delay)
		self._loaded = False

	def _loadFinished(self, result):
		self._loaded = True

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

url = "http://g1.globo.com"
response = requests.get(url)
# parse html
page = str(BeautifulSoup(response.content))

datetime_crawl = datetime.now()

# s = Screenshot()
# s.capture(url, './output/G1Home_'+ datetime_crawl.strftime("%Y_%m_%d_%H_%M_%S") +'.png')

db = MySQLdb.connect("localhost","root","","stream" )
cursor = db.cursor()

# using set to prevent duplicate url's
urls = set()

while True:
	url, n = getURL(page)
	page = page[n:]
	if url:
		urls.add(url)
	else:
		break

for url in urls:
	cursor.execute(""" insert into home_g1
		(url, datetime_crawl, url_md5) 
		values(%s, %s, md5(%s)) ;""" , [url, datetime_crawl, url] )

db.commit()
