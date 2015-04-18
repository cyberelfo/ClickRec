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

# parser = argparse.ArgumentParser()
# parser.add_argument("document_id", 
#     help="Source document to generate recommendations")
# args = parser.parse_args()
# document_id = args.document_id


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

        # if annotations == ['0']:
        #     print "ANNOTATIONS:"+document_id
        r.rpush("ANNOTATIONS:"+document_id, *annotations)
        r.rpush("SECTIONS:"+document_id, *sections)
        miss = 1

    return miss, annotations, sections

def similar_sections(document_id):
    similar = []
    bag_of_words = []

    miss, annotations, sections = get_annotations(document_id)
    # import pdb; pdb.set_trace()
    if sections == ['0']:
        return sections

    print
    print '#########'
    print "Sections:", sections
    print

    frequents = pickle.loads(r.get('FREQS:sections:'+str(fi_size)))

    d, model, index = tfidf.model(frequents)

    sims = tfidf.query(sections, d, model, index)
    d_sims = dict(sims) 
    for frequent in frequents:
        bag_of_words.extend(list(frequent))

    counter_bag_of_words = Counter(bag_of_words)

    N = len(frequents)
    for i, frequent in enumerate(frequents):
        # tfidf = 0
        # for section in sections:
        #     if section in frequent:
        #         tf = 1.0
        #         idf = math.log(N / float((counter_bag_of_words[section])))
        #         tfidf += tf * idf
        jaccard = len(set(sections) & frequent) / float(len(set(sections) | frequent))

        similar.append((d_sims[i], jaccard, frequent))        

    return similar

def similar_annotations(document_id):
    similar = []
    bag_of_words = []

    miss, annotations, sections = get_annotations(document_id)
    if annotations == ['0']:
        print "Document has no annotation"
        return similar

    print
    print "############"
    print "Annotations:", annotations
    print

    frequents = pickle.loads(r.get('FREQS:uris:'+str(fi_size)))


    for frequent in frequents:
        bag_of_words.extend(list(frequent))

    counter_bag_of_words = Counter(bag_of_words)

    for frequent in frequents:
        tfidf = 0
        for annotation in annotations:
            if annotation in frequent:
                tf = 1.0
                idf = math.log(len(frequents) / float((counter_bag_of_words[annotation] + 1)))
                tfidf += tf * idf
        jaccard = len(set(annotations) & frequent) / float(len(set(annotations) | frequent))
        similar.append((tfidf, jaccard, frequent))

    return similar


def recommend_sparql(freq_annotations):

    i_recommendations = iter(freq_annotations)
    rec = next(i_recommendations)
    query_where = '{?s ?p <'+rec+'> }\n'
    for rec in i_recommendations:
        query_where += 'UNION {?s ?p <'+rec+'> }\n'

    query = """ 
    SELECT ?s count(?s) as ?qty
    FROM <http://semantica.globo.com/esportes/>
    WHERE {?s a <http://semantica.globo.com/esportes/MateriaEsporte> .
           { %s }
    } 
    GROUP BY ?s
    ORDER BY DESC(?qty)
    LIMIT 10
    """ % (query_where)

    print query

    articles = run_query(query)

    print
    print "Qty annotations - News article URL"
    print "----------------------------------"
    for article in articles:
        print article['qty']['value'], article['s']['value']

    return articles

def calc(document_id):
    global r, s
    print "Program start..."
    start = dt.now()

    s = solr.SolrConnection(solr_endpoint)

    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    url = r.get("URL:"+document_id)

    print "URL:", url 

    similar = similar_sections(document_id)

    # import pdb; pdb.set_trace()

    for tfidf, jaccard, frequent in sorted(similar, reverse=True):
        if tfidf + jaccard > 0:
            print tfidf, jaccard, frequent

    print
    similar = similar_annotations(document_id)

    for tfidf, jaccard, frequent in sorted(similar, reverse=True):
        if tfidf + jaccard > 0:
            print tfidf, jaccard, frequent

    # if len(freq_annotations) > 0:
    #     articles = recommend_sparql(freq_annotations)
    # else:
    #     print "Document annotations not found in frequent itemsets"

    stop = dt.now()
    execution_time = stop - start 

    print
    print "End processing"
    print "Execution time:", execution_time

    return [4,5,6,7]


if __name__ == '__main__':
    main()
    # import cProfile
    # cProfile.run('main()')
