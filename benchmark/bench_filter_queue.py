#!/usr/bin/env python
# encoding: utf-8

import time
from yascrapy.filter_queue import FilterQueue
from yascrapy.bloomd import get_client
from bench_utils import get_logger


def test():
    bloomd_client = get_client()
    crawler_name = "test_crawler_1"
    queue = FilterQueue(bloomd_client=bloomd_client, crawler_name=crawler_name)
    page_cnt = 50000
    cnt = 0
    logger = get_logger("test_queue")
    start_time = time.time()
    for i in xrange(1, page_cnt + 1):
        url = "http://stackoverflow.com/users?page=%d&tab=reputation&filter=week" % i
        cnt += 1
        if cnt % 1000 == 0:
            logger.info(str(cnt))
        queue.push(url)
    end_time = time.time()
    print "start time: ", start_time
    print "end time: ", end_time
    print "speed: %f times/second" % (page_cnt / (end_time - start_time))

if __name__ == "__main__":
    test()
