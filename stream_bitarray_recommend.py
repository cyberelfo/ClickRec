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
num_results = 10
num_frequents = 10

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

def similar_frequent(frequents, query):
    similar = []

    # import pdb; pdb.set_trace()

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

    documents = []
    miss, annotations, sections = get_annotations(document_id)

    if prefix == 'SECTIONS:':
        freqs_prefix = 'sections:'
        query = sections
    elif prefix == 'ANNOTATIONS:':
        freqs_prefix = 'uris:'
        query = annotations

    if query == ['0']:
        # print "Document has no section"
        return query

    frequents = pickle.loads(r.get('FREQS:'+freqs_prefix+str(fi_size)))

    similar = similar_frequent(frequents, query)

    for tfidf, jaccard, frequent in sorted(similar, reverse=True):
        recommendations = recommend_tfidf(document_id, frequent, prefix)
        break

    count = 0
    for tfidf, docid in sorted(recommendations, reverse=True):
        count += 1
        if count > num_results:
            break

        documents.append(docid)

    return documents

def recommend_pages_2(document_id, prefix, fi_size):

    documents = []

    itemset = set()

    miss, annotations, sections = get_annotations(document_id)

    if prefix == 'SECTIONS:':
        freqs_prefix = 'sections:'
        query = sections
    elif prefix == 'ANNOTATIONS:':
        freqs_prefix = 'uris:'
        query = annotations

    if query == ['0']:
        # print "Document has no section"
        return query

    frequents = pickle.loads(r.get('FREQS:'+freqs_prefix+str(fi_size)))

    similar = similar_frequent(frequents, query)

    count = 0
    for tfidf, jaccard, frequent in sorted(similar, reverse=True):
        count += 1
        if count > num_frequents:
            break
        itemset |= frequent

    new_itemset = itemset - set(query)

    # import pdb; pdb.set_trace()

    recommendations = recommend_tfidf(document_id, new_itemset, prefix)

    count = 0
    for tfidf, docid in sorted(recommendations, reverse=True):
        count += 1
        if count > num_results:
            break

        documents.append(docid)

    return documents

def calc(document_id, r_type):
    global r, s

    s = solr.SolrConnection(solr_endpoint)

    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    url = r.get("URL:"+document_id)

    # print "URL:", url 

    if r_type == 1:
        documents = recommend_pages(document_id, 'SECTIONS:', fi_size)
    elif r_type == 2:
        documents = recommend_pages_2(document_id, 'SECTIONS:', fi_size)
    elif r_type == 3:
        documents = recommend_pages(document_id, 'ANNOTATIONS:', fi_size)
    elif r_type == 4:
        documents = recommend_pages_2(document_id, 'ANNOTATIONS:', fi_size)

    return documents


if __name__ == '__main__':
    # import cProfile
    # cProfile.run('main()')

    parser = argparse.ArgumentParser()
    parser.add_argument("document_id", 
        help="Source document to generate recommendations")
    args = parser.parse_args()
    document_id = args.document_id

    print "Program start..."
    start = dt.now()

    print calc(document_id, 1)
    print calc(document_id, 2)
    print calc(document_id, 3)
    print calc(document_id, 4)

    stop = dt.now()
    execution_time = stop - start 

    print
    print "End processing"
    print "Execution time:", execution_time
