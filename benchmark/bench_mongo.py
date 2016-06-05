#!/usr/bin/env python
# encoding: utf-8
import time
from bench_utils import get_logger
import json
import pymongo

try:
    settings_f = open('/etc/yascrapy/config.json')
except Exception as e:
    print e

settings = json.load(settings_f)

def test():
    item_cnt = 10000
    cnt = 0
    logger = get_logger("test_queue")
    start_time = time.time()
    mongo_cli = pymongo.MongoClient(host=settings['MongoIp'], port=settings['MongoPort'])
    mongo_cli.test.user.ensure_index('id')
    for i in xrange(1, item_cnt + 1):
        item = {
            "id": i,
            "url": "http://stackoverflow.com/users?page=%d&tab=reputation&filter=week" % i,
        }
        cnt += 1
        if cnt % 1000 == 0:
            logger.info(str(cnt))
        mongo_cli.test.user.update({'id': item['id']}, item, True)
    end_time = time.time()
    print "start time: ", start_time
    print "end time: ", end_time
    print "speed: %f times/second" % (item_cnt / (end_time - start_time))

if __name__ == "__main__":
    test()
