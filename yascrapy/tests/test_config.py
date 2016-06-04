#!/usr/bin/python
# coding: utf-8
import unittest
from yascrapy.request_queue import RequestQueue
from yascrapy.request_queue import Request
from yascrapy.response_queue import ResponseQueue
from yascrapy.filter_queue import FilterQueue
from yascrapy.ssdb import get_clients
from yascrapy.rabbitmq import create_conn
from yascrapy.config import Config
from yascrapy import bloomd


class TestQueues(unittest.TestCase):

    def setUp(self):
        cfg = Config().get()
        self.ssdb_clients = get_clients(nodes=cfg["SSDBNodes"])
        conn = create_conn(cfg)
        self.crawler_name = 'test_crawler'
        self.bloomd_client = bloomd.get_client(nodes=cfg["BloomdNodes"])
        self.filter_q = FilterQueue(
            crawler_name=self.crawler_name, 
            bloomd_client=self.bloomd_client
        )
        self.req_q = RequestQueue(
            self.crawler_name, 
            ssdb_clients=self.ssdb_clients, 
            filter_q=self.filter_q
        )
        self.resp_q = ResponseQueue(
            self.crawler_name, 
            ssdb_clients=self.ssdb_clients,
        )
        self.publish_channel = conn.channel()
        self.req_d = {
            'crawler_name': self.crawler_name,
            'url': 'http://stackoverflow.com/users/1144035/gordon-linoff',
            'proxy_name': 'http_china',
            'method': 'GET',
            'headers': {},
            'files': None,
            'data': None,
            'params': {},
            'auth': None,
            'cookies': {},
            'hooks': None,
            'json': None,
            'timeout': 10,
        }
        self.req = Request(**self.req_d)


    def test_config(self):
        cfg = Config().get()
        self.assertTrue("RabbitmqIp" in cfg)
        self.assertTrue("RabbitmqPort" in cfg)
        self.assertTrue("MongoIp" in cfg)
        self.assertTrue("MongoPort" in cfg)

    def test_request_queue_safe_push(self):
        self.req_q.push(self.req, self.publish_channel)
        self.req_q.safe_push(self.req, self.publish_channel)

if __name__ == '__main__':
    unittest.main()
