#!/usr/bin/python
# coding: utf-8
import unittest
import json
from yascrapy.response_queue import Response
from yascrapy.response_queue import Selector
import os

class TestResponse(unittest.TestCase):
    def setUp(self):
        html_file = os.path.join(os.path.dirname(__file__), "test.html")
        with open(html_file, "r") as f:
            html = f.read()
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
        self.resp = Response()
        self.resp.from_json(json.dumps(self.resp_d))

    def test_selector(self):
        from lxml.html import fromstring
        html = '''<div class="column one-fourth vcard" itemscope="" itemtype="http://schema.org/Person">
        <a href="https://avatars2.githubusercontent.com/u/15075413?v=3&amp;s=400" aria-hidden="true" class="vcard-avatar d-block position-relative" itemprop="image"><img alt="" class="avatar rounded-2" height="230" src="https://avatars0.githubusercontent.com/u/15075413?v=3&amp;s=460" width="230"/></a>

      <h1 class="vcard-names my-3">
  <div class="vcard-fullname" itemprop="name"/>
  <div class="vcard-username" itemprop="additionalName">Ricardo1970</div>
</h1>



<ul class="vcard-details border-top border-gray-light py-3">
      

  
  
  <li class="vcard-detail py-1 css-truncate css-truncate-target"><svg aria-hidden="true" class="octicon octicon-clock" height="16" role="img" version="1.1" viewbox="0 0 14 16" width="14"><path d="M8 8h3v2H7c-0.55 0-1-0.45-1-1V4h2v4z m-1-5.7c3.14 0 5.7 2.56 5.7 5.7S10.14 13.7 7 13.7 1.3 11.14 1.3 8s2.56-5.7 5.7-5.7m0-1.3C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7S10.86 1 7 1z"/></svg><span class="join-label">Joined on </span><time class="join-date" datetime="2015-10-11T13:29:47Z" day="numeric" is="local-time" month="short" year="numeric">Oct 11, 2015</time></li>
</ul>


      <div class="vcard-stats border-top border-bottom border-gray-light mb-3 py-3">
        <a class="vcard-stat" href="/Ricardo1970/followers">
          <strong class="vcard-stat-count d-block">0</strong>
          <span class="text-muted">Followers</span>
        </a>
        <a class="vcard-stat" href="/stars/Ricardo1970">
          <strong class="vcard-stat-count d-block">0</strong>
          <span class="text-muted">Starred</span>
        </a>
        <a class="vcard-stat" href="/Ricardo1970/following">
          <strong class="vcard-stat-count d-block">0</strong>
          <span class="text-muted">Following</span>
        </a>
      </div>

    </div>
    '''
        root = fromstring(html)
        s = Selector(root)
        baseInfo_div = s.css(".vcard")[0]
        des = {}
        des["name"] = baseInfo_div.css(".vcard-fullname")
        des["login"] = baseInfo_div.css(".vcard-username")
        des["company"] = baseInfo_div.css('li[itemprop="worksFor"]')
        des["location"] = baseInfo_div.css('li[itemprop="homeLocation"]')
        des["email"] = baseInfo_div.css(".email")
        des["blog"] =  baseInfo_div.css(".url")
        for i in des:
            try:
              des[i] = des[i].extract()[0]
            except:
              des[i] = None
        print des
        self.assertEqual(des["login"], "Ricardo1970")

    def test_xpath(self):
        res = []
        res.append(self.resp.xpath('//h2[@class="user-card-name"]/text()').extract())
        res.append(self.resp.xpath('//h2[@class="user-card-name"]').xpath('*[@class="top-badge"]/a/@href').extract())
        for each in res:
            self.assertNotEqual(each, [])

    def test_css(self):
        res = []
        res.append(self.resp.css(".item-summary").extract())
        for each in res:
            self.assertNotEqual(each, [])
        div = self.resp.css(".badges")[0]
        res = []
        res.append(div.css(".badgecount").extract())
        for each in res:
            self.assertNotEqual(each, [])

if __name__ == '__main__':
    unittest.main()
