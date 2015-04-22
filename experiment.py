#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime as dt
import MySQLdb
import stream_bitarray_recommend as sbr
import redis
import logging

user_path_size = 2
filename = 'rt-actions-read-2015_01_14_00.log'
product_id = 2

def main():

    # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename='/tmp/experiment.log',
                        filemode='w')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    logging.getLogger("gensim").setLevel(logging.ERROR)

    logging.debug('Program starting...')

    start = dt.now()

    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    topnews = [i[8:] for i in r.zrevrange('DOC_COUNTS', 0, 9)]

    db = MySQLdb.connect("localhost","root","","stream" )
    cursor = db.cursor()


    sql = """ select user_id, count(*) 
            from stream 
            where product_id = %s
            and filename = '%s'
            group by user_id
            having count(*) >= %s
            -- limit 1000
            ;
        """ % (product_id, filename, user_path_size)

    logging.info("Selecting users...")

    cursor.execute(sql)

    logging.info("Recommending and checking...")

    hit = 0
    hit_topnews = 0
    count = 0

    same_hit_rec = 0
    more_hit_rec = 0
    less_hit_rec = 0

    same_hit_top = 0
    more_hit_top = 0
    less_hit_top = 0

    for user in cursor:
        count += 1

        sql = """ select document_id 
                from stream 
                where product_id = 2
                and filename = 'rt-actions-read-2015_01_14_00.log'
                and user_id = %s
                order by stream_datetime;
            """ % (user[0])

        cursor.execute(sql)
        documents = cursor.fetchall()

        documents = [str(i[0]) for i in documents]

        head = documents[0]
        tail = set(documents[1:])

        logging.debug("User path: %s", documents)

        recommendation = sbr.calc(head, 1)

        logging.debug("Recs: %s", recommendation)

        intersect_rec = set(recommendation) & tail

        if len(intersect_rec) > 0:
            hit += 1
            logging.debug("Hit! Size: %s", len(intersect_rec))

        logging.debug("TopNews: %s", topnews)

        intersect_topnews = set(topnews) & tail

        if len(intersect_topnews) > 0:
            hit_topnews += 1
            logging.debug("Hit! Size: %s", len(intersect_topnews))


        if count % 100 == 0:
            logging.info("Total users: %s ClickRec Hits: %s TopNews Hits: %s", count, hit, hit_topnews)

        if len(intersect_rec) > 0:
            if intersect_rec == intersect_topnews:
                same_hit_rec +=1
            elif intersect_rec > intersect_topnews:
                more_hit_rec += 1
            elif intersect_rec < intersect_topnews:
                less_hit_rec += 1

        if len(intersect_topnews) > 0:
            if intersect_topnews == intersect_rec:
                same_hit_top +=1
            elif intersect_topnews > intersect_rec:
                more_hit_top += 1
            elif intersect_topnews < intersect_rec:
                less_hit_top += 1


    logging.info('')
    logging.info('### Results ###')
    logging.info("Total users: %s Hits: %s Hits TopNews: %s", count, hit, hit_topnews)
    logging.info("ClickRec performance")
    logging.info("same_hit: %s more_hit: %s less_hit: %s", same_hit_rec, more_hit_rec, less_hit_rec)
    logging.info("TopNews performance")
    logging.info("same_hit: %s more_hit: %s less_hit: %s", same_hit_top, more_hit_top, less_hit_top)

    stop = dt.now()
    execution_time = stop - start 

    logging.info("End processing")
    logging.info("Execution time: %s", execution_time)


if __name__ == '__main__':
    main()
