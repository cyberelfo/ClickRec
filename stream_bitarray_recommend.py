#!/usr/bin/env python
# -*- coding: utf-8 -*-

import solr
import argparse
import ConfigParser
import redis
import pickle
from datetime import datetime as dt

config = ConfigParser.ConfigParser()
config.read("./stream.ini")
solr_endpoint = config.get('main', 'solr_endpoint')

parser = argparse.ArgumentParser()
parser.add_argument("document_id", 
    help="Number of users / size of the window")
args = parser.parse_args()
document_id = args.document_id

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

def sections_profile(document_id):
    profile = set()

    miss, annotations, sections = get_annotations(document_id)
    # import pdb; pdb.set_trace()
    if sections == ['0']:
        return sections

    print
    print '#########'
    print "Sections:", sections
    print
    frequents = pickle.loads(r.get('FREQS:sections:2'))

    # import pdb; pdb.set_trace()

    for section in sections:
        for frequent in frequents:
            if section in frequent:
                # print str((frequent - set([profile])))
                # profile |= (frequent - set([section]))
                profile.add(frequent)

    print "Profile:"
    for p in profile:
        print p

    return profile

def frequent_annotations(document_id):
    freq_annotations = set()

    miss, annotations, sections = get_annotations(document_id)
    if annotations == ['0']:
        print "Document has no annotation"
        return freq_annotations

    print
    print "############"
    print "Annotations:", annotations
    print
    frequents = pickle.loads(r.get('FREQS:uris:2'))

    # import pdb; pdb.set_trace()

    for annotation in annotations:
        for frequent in frequents:
            if annotation in frequent:
                print frequent
                freq_annotations |= (frequent - set([annotation]))
                # recommendations.add(frequent)

    return freq_annotations


def recommend(freq_annotations):

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

    articles = run_query(query)

    print
    print "Qty annotations - News article URL"
    print "----------------------------------"
    for article in articles:
        print article['qty']['value'], article['s']['value']

    return articles

def main():
    global r, s
    print "Program start..."
    start = dt.now()

    s = solr.SolrConnection(solr_endpoint)

    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    url = r.get("URL:"+document_id)

    print "URL:", url 

    profile = sections_profile(document_id)

    print
    freq_annotations = frequent_annotations(document_id)

    if len(freq_annotations) > 0:
        articles = recommend(freq_annotations)
    else:
        print "Document annotations not found in frequent itemsets"

    stop = dt.now()
    execution_time = stop - start 

    print
    print "End processing"
    print "Execution time:", execution_time


if __name__ == '__main__':
    main()
    # import cProfile
    # cProfile.run('main()')
