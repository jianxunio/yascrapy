# -*- coding: utf-8 -*-
from yascrapy.request_queue import Request
from yascrapy.base import BaseProducer
import random


class Producer(BaseProducer):

    def run(self):
        page_cnt = 132611
        # page_cnt = 132
        cnt = 0
        for i in xrange(1, page_cnt + 1):
            url = "http://wh.meituan.com/?id=%s" % i
            cnt += 1
            if cnt % 1000 == 0:
                print cnt
            r = Request(
                url=url,
                timeout=15,
                crawler_name=self.crawler_name,
                proxy_name='http_china',
                method='GET',
                params={},
                data=''
            )
            q = random.choice(self.req_queues)
            q.safe_push(r, self.publish_channel)
