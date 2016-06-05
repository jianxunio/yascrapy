# -*- coding: utf-8 -*-
from yascrapy.ssdb import get_proxy_client
from yascrapy.config import Config
from yascrapy.plugins.handle_proxy import Plugin
import unittest

class TestProxyHandler(unittest.TestCase):
    def setUp(self):
        self.proxy_name = "http_china"
        self.cfg = Config().get()
        self.proxy_client = get_proxy_client(cfg=self.cfg)
        fake_worker = type('Worker', (object,), {})
        setattr(fake_worker, "proxy_name", self.proxy_name)
        setattr(fake_worker, "proxy_client", self.proxy_client)
        self.proxy_handler = Plugin(fake_worker)


    def test_get_proxy(self):
        p = self.proxy_handler.get()
        self.assertNotEqual(p, None)

    def test_del_proxy(self):
        p = self.proxy_handler.get()
        self.assertNotEqual(p, None)
        ok = self.proxy_handler.del_proxy(p)
        self.assertEqual(ok, True)

if __name__ == "__main__":
    unittest.main()
