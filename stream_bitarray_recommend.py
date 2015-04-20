#!/usr/bin/env python
# -*- coding: utf-8 -*-

import solr
import argparse
import ConfigParser
import redis
import pickle
from datetime import datetime as dt
from collections import Counter
import math
import tfidf

config = ConfigParser.ConfigParser()
config.read("./stream.ini")
solr_endpoint = config.get('main', 'solr_endpoint')
max_fi_size = config.getint('main', 'max_fi_size')

fi_size = max_fi_size

def run_query(query):
    from SPARQLWrapper import SPARQLWrapper, JSON
    _sparql = SPARQLWrapper("http://staging.semantica.globoi.com/sparql/")
    _sparql.setQuery(query)
    _sparql.setReturnFormat(JSON)
    results = _sparql.query().convert()
    return results["results"]["bindings"]

def get_annotations(document_id):

    annotation_found = r.exists("ANNOTATIONS:"+document_id)
    if annotation_found:
        annotations = r.lrange("ANNOTATIONS:"+document_id, 0, -1)
        sections = r.lrange("SECTIONS:"+document_id, 0, -1)
        miss = 0

    else:
        response = s.query('documentId:'+str(document_id), fields='entity, section')

        annotations = ['0']
        sections = ['0']

        if response.numFound == 1:
            if 'entity' in response.results[0]:
                annotations = response.results[0]['entity']
            if 'section' in response.results[0]:
                sections = response.results[0]['section']

        r.rpush("ANNOTATIONS:"+document_id, *annotations)
        r.rpush("SECTIONS:"+document_id, *sections)
        miss = 1

    return miss, annotations, sections

def similar_frequent(document_id, prefix, fi_size):
    similar = []

    miss, annotations, sections = get_annotations(document_id)

    if prefix == 'sections:':
        query = sections
    elif prefix == 'uris:':
        query = annotations

    if query == ['0']:
        print "Document has no section"
        return query

    print
    print '#########'
    print "Query:", query
    print

    # import pdb; pdb.set_trace()
    frequents = pickle.loads(r.get('FREQS:'+prefix+str(fi_size)))

    d, model, index = tfidf.model(frequents)
    sims = tfidf.query(query, d, model, index)
    d_sims = dict(sims) 

    for i, frequent in enumerate(frequents):
        jaccard = len(set(query) & frequent) / float(len(set(query) | frequent))
        similar.append((d_sims[i], jaccard, frequent))        

    return similar

def recommend_tfidf(document_id, itemset, prefix):

    window_size = 100000
    vals = []
    recommendations = []
    keys = r.zrevrangebyscore('DOC_COUNTS', '+inf', window_size * 0.01)

    # Avoid recommending the document the user just saw.
    try:
        keys.remove('BIT_DOC:'+document_id)
    except ValueError:
        pass  # do nothing!

    for k in keys:
        docid = k[len('BIT_DOC:'):]
        vals.append(r.lrange(prefix+docid, 0, -1))

    d, model, index = tfidf.model(vals)
    sims = tfidf.query(itemset, d, model, index)
    d_sims = dict(sims)

    # import pdb; pdb.set_trace()

    for i, k in enumerate(keys):
        recommendations.append((d_sims[i], k[len('BIT_DOC:'):]))

    return recommendations


def recommend_pages(document_id, prefix, fi_size):

    print

    if prefix == 'SECTIONS:':
        similar_prefix = 'sections:'
    elif prefix == 'ANNOTATIONS:':
        similar_prefix = 'uris:'

    similar = similar_frequent(document_id, similar_prefix, fi_size)

    print "TFIDF JACCARD ITEMSET"
    for tfidf, jaccard, frequent in sorted(similar, reverse=True):
        print tfidf, jaccard, frequent
        recommendations = recommend_tfidf(document_id, frequent, prefix)
        print
        break

    print "TFIDF DOCID URL"
    count = 0
    for tfidf, docid in sorted(recommendations, reverse=True):
        count += 1
        if count > 10:
            break
        print tfidf, docid, r.get("URL:"+docid)

def calc(document_id):
    global r, s
    print "Program start..."
    start = dt.now()

    s = solr.SolrConnection(solr_endpoint)

    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    url = r.get("URL:"+document_id)

    print "URL:", url 

    recommend_pages(document_id, 'SECTIONS:', fi_size)

    # import pdb; pdb.set_trace()

    recommend_pages(document_id, 'ANNOTATIONS:', fi_size)

    stop = dt.now()
    execution_time = stop - start 

    print
    print "End processing"
    print "Execution time:", execution_time

    return [4,5,6,7]


if __name__ == '__main__':
    # import cProfile
    # cProfile.run('main()')

    parser = argparse.ArgumentParser()
    parser.add_argument("document_id", 
        help="Source document to generate recommendations")
    args = parser.parse_args()
    document_id = args.document_id

    calc(document_id)
