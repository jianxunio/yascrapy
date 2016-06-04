#!/usr/bin/env python
# encoding: utf-8

import logging
from yascrapy.base import BaseWorker
from settings import TestItem


class Worker(BaseWorker):

    def callback(self, channel, method, properties, body):
        resp_key = body
        response, err = self.resp_q.get(resp_key)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        if response is None:
            logging.warning("no match response to response_key %s" % resp_key)
            return
        logging.info('status code: %s url: %s' %
                     (response.status_code, response.url))
        if self.error_handler.handle_downloader_error(response):
            return
        if self.error_handler.handle_status_error(response):
            return

        if '/users/' in response.url:
            item = self._user_parse(response)
        self.db_handler.update(item)

    def _user_parse(self, response):
        item = TestItem()
        item['uid'] = int(response.url.split('/')[-2])
        item['name'] = response.xpath(self.name_xpath).extract()[0].strip()
        return item
