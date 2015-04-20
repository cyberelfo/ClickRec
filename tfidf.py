#!/usr/bin/env python
# -*- coding: utf-8 -*-

from gensim import corpora, models, similarities

def model(frequents):
	d = corpora.Dictionary(frequents)
	corpus = [d.doc2bow(doc) for doc in frequents]

	# model = models.LogEntropyModel(corpus)
	# model = models.LsiModel(corpus)
	model = models.TfidfModel(corpus)
	index = similarities.MatrixSimilarity(model[corpus])

	return d, model, index

def query(query, d, model, index):

	tfidf_q = model[d.doc2bow(query)]
	sims = index[tfidf_q]
	sims = sorted(enumerate(sims), key=lambda item: -item[1])

	return sims

if __name__ == '__main__':

	frequents = [['globoesporte', 'sp', 'futebol', 'times'], ['palmeiras', 'times', 'futebol', 'globoesporte'], ['globoesporte', 'times', 'sp', 'copa SP de futebol j\xc3\xbanior'], ['globoesporte', 'times', 'sp', 'tem esporte']]
	query = ['campeonato paulista', 'futebol', 'globoesporte', 'palmeiras', 'sp', 'times']

	d, model, index = model(frequents)
	sims = query(query, d, model, index)
	print(sims)
