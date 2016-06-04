#!/usr/bin/python
# coding: utf-8
import redis
import logging


class Plugin(object):

    def __init__(self, worker):
        '''Proxy queue is put on redis, use `set` structure.'''
        proxy_name = worker.proxy_name
        proxy_client = worker.proxy_client
        if proxy_client is None:
            raise Exception("proxy_client can not be None")
        self.proxy_client = proxy_client
        self.proxy_name = proxy_name
        self.name = "proxy_handler"

    def del_proxy(self, proxy):
        k = 'http_proxy:%s' % self.proxy_name
        r = redis.Redis(connection_pool=self.proxy_client["connection_pool"])
        res = r.srem(k, proxy)
        if res:
            logging.info('delete proxy: %s' % proxy)
            return True
        else:
            logging.warn('the proxy %s is not exist!' % proxy)
            return False

    def get(self):
        """Get random proxy from proxy queue in ssdb, not add benchmark."""
        k = 'http_proxy:%s' % self.proxy_name
        r = redis.Redis(connection_pool=self.proxy_client["connection_pool"])
        return r.srandmember(k)
