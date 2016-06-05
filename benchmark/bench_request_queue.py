#!/usr/bin/env python
# encoding: utf-8
import time
from yascrapy.request_queue import RequestQueue
from yascrapy.request_queue import Request
from yascrapy.filter_queue import FilterQueue
from yascrapy.ssdb import get_clients
from yascrapy.config import Config
from yascrapy.rabbitmq import create_conn
from yascrapy import bloomd
from bench_utils import get_logger


def test():
    crawler_name = "weibo"
    cfg = Config().get()
    ssdb_clients, ring = get_clients(nodes=cfg["SSDBNodes"])
    conn = create_conn(cfg)
    bloomd_client = bloomd.get_client(nodes=cfg["BloomdNodes"]) 
    filter_q = FilterQueue(bloomd_client=bloomd_client, crawler_name=crawler_name)
    queue_name = "http_request:%s:test" % crawler_name
    queue = RequestQueue(crawler_name, ssdb_clients=ssdb_clients, filter_q=filter_q, queue_name=queue_name)
    ch = conn.channel()
    ch.exchange_declare(
        exchange=crawler_name, 
        exchange_type="topic", 
        durable=True
    )
    ch.queue_declare(
        queue=queue_name,
        durable=True
    )
    ch.queue_bind(
        exchange=crawler_name, 
        queue=queue_name, 
        routing_key=queue_name
    )
    ch.close()
    publish_channel = conn.channel()
    publish_channel.confirm_delivery()
    page_cnt = 50000
    cnt = 0
    logger = get_logger("test_queue")
    start_time = time.time()
    for i in xrange(1, page_cnt + 1):
        url = "http://stackoverflow.com/users?page=%d&tab=reputation&filter=week" % i
        cnt += 1
        if cnt % 1000 == 0:
            logger.info(str(cnt))
        r = Request(url=url, timeout=15, headers={}, crawler_name=crawler_name)
        queue.push(r, publish_channel)
    end_time = time.time()
    print "start time: ", start_time
    print "end time: ", end_time
    print "speed: %f times/second" % (page_cnt / (end_time - start_time))

if __name__ == "__main__":
    test()
