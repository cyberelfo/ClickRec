#!/usr/bin/env python
# -*- coding: utf-8 -*-

import solr
import argparse
import ConfigParser
import redis
import pickle

config = ConfigParser.ConfigParser()
config.read("./stream.ini")
solr_endpoint = config.get('main', 'solr_endpoint')

parser = argparse.ArgumentParser()
parser.add_argument("document_id", 
    help="Number of users / size of the window")
args = parser.parse_args()
document_id = args.document_id


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

def recommend(fi_type, document_id):
    recommendations = set()

    miss, annotations, sections = get_annotations(document_id)
    # import pdb; pdb.set_trace()
    if annotations == ['0']:
        return recommendations

    # annotations_combinations = [set(i) for i in combinations(annotations, 2)]



    print "annotations:", annotations
    print "sections:", sections
    print
    frequents = pickle.loads(r.get('FREQS:'+fi_type+':2'))

    if fi_type == "uris":
        items = annotations
    else:
        items = sections

    for item in items:
        for frequent in frequents:
            if item in frequent:
                recommendations.add(frequent - set([item]))

    return recommendations

def main():
    global r, s
    print "Program start..."

    s = solr.SolrConnection(solr_endpoint)

    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    print
    recommendation = recommend('uris',document_id)
    print recommendation

    print
    recommendation = recommend('sections',document_id)
    print recommendation

if __name__ == '__main__':
    main()
    # import cProfile
    # cProfile.run('main()')
