# -*- coding: utf-8 -*-
import unittest
import json
from yascrapy.plugins.handle_error import Plugin
from yascrapy.ssdb import get_clients
from yascrapy.response_queue import Response
from yascrapy.filter_queue import FilterQueue
from yascrapy.request_queue import RequestQueue
from yascrapy import bloomd
from yascrapy.config import Config
from yascrapy.rabbitmq import create_conn
import os
import chardet

class TestErrorHandler(unittest.TestCase):
    def setUp(self):
        self.crawler_name = 'test'
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
        test_html_file = os.path.join(os.path.dirname(__file__), "test.html")
        with open(test_html_file, 'r') as f:
            html = f.read()

        self.resp_d = {
            'crawler_name': self.crawler_name,
            'http_request': json.dumps(self.req_d),
            'error_code': 0,
            'error_msg': '',
            'status_code': 200,
            'reason': 'OK',
            'html': html,
            'cookies': {},
            'url': 'http://stackoverflow.com/users/1144035/gordon-linoff',
            'headers': {},
            'encoding': None,
            'elapsed': None,
            'http_proxy': '127.0.0.1:8000'
        }
        cfg = Config().get()
        self.ssdb_clients = get_clients(nodes=cfg["SSDBNodes"])
        conn = create_conn(cfg)
        self.publish_channel = conn.channel()
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
        html_404_strings = [['Page', 'Not', 'Found'], [u"页面不存在"]]
        fake_worker = type("Worker", (object, ), {})
        setattr(fake_worker, "crawler_name", self.crawler_name)
        setattr(fake_worker, "req_q", self.req_q)
        setattr(fake_worker, "publish_channel", self.publish_channel)
        setattr(fake_worker, "html_404_strings", html_404_strings)
        self.error_handler = Plugin(fake_worker)

    def test_404_err(self):
        err_html_file = os.path.join(os.path.dirname(__file__), '404_err.html')
        self.handle_error_file(err_html_file)

    def test_zh_404_err(self):
        err_html_file = os.path.join(os.path.dirname(__file__), "zh_404_err.html")
        self.handle_error_file(err_html_file)

    def handle_error_file(self, filename):
        with open(filename, "r") as f:
            html = f.read()
        encoding = chardet.detect(html)["encoding"]
        html = html.decode(encoding)

        self.err_resp_d = {
            'crawler_name': self.crawler_name,
            'http_request': json.dumps(self.req_d),
            'error_code': 0,
            'error_msg': '',
            'status_code': 404,
            'reason': 'Not Found',
            'html': html,
            'cookies': {},
            'url': 'http://stackoverflow.com/test',
            'headers': {},
            'encoding': None,
            'elapsed': None,
            'http_proxy': '127.0.0.1:8000'
        }

        resp = Response()
        resp.from_json(json.dumps(self.err_resp_d))
        self.assertTrue(self.error_handler.handle_err(resp))

        wrong_err_resp = self.resp_d    # 不是正常的404页面
        wrong_err_resp['status_code'] = 404
        resp.from_json(json.dumps(wrong_err_resp))
        self.assertFalse(self.error_handler.handle_err(resp))

if __name__ == "__main__":
    unittest.main()
