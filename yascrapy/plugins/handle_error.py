#!/usr/bin/python
# coding: utf-8
import logging
import json
from ..request_queue import Request


class Plugin(object):

    def __init__(self, worker):
        crawler_name = worker.crawler_name
        request_queue = worker.req_q
        publish_channel = worker.publish_channel
        html_404_strings = worker.html_404_strings

        if request_queue is None:
            raise Exception("request_queue cannot be None")
        if publish_channel is None:
            raise Exception("publish_channel cannot be None")
        self.crawler_name = crawler_name
        self.html_404_strings = html_404_strings
        self.request_queue = request_queue
        self.handle_func_map = {
            '404': self._handle_404,
            '403': self._handle_403,
            '302': self._handle_302,
            '301': self._handle_301,
        }
        self.publish_channel = publish_channel
        self.name = "error_handler"

    def handle_page_error(self, response):
        # crawler_name = response.url.split('/')[2].split('.')[-2]
        content = 'Error url: %s\n' % response.url
        content += 'Error detail: Missing some information in page, should check whether page structure changes'
        logging.error(content)
        self.request_queue.error_push_cache(
            Request(**json.loads(response.http_request))
        )

    def handle_item_error(self, response, struct_item, necessary_fields=[]):
        """
        Args:
            response: 
                Response object
            struct_item: 
                Item object defined in `items.py` with specific crawler
            necessary_fields: 
                `struct_item` must contain all fields in this list

        Returns:
            True if error exsits, False if no error
        """
        item = list(struct_item) if isinstance(
            struct_item, types.GeneratorType) else struct_item
        # code is not explict and readable
        all_right = all(map(lambda x: item.get(x, ''), necessary_fields))
        if not all_right:
            crawler_name = response.url.split('/')[2].split('.')[-2]
            sub = '%s error' % crawler_name
            content = 'Error url: %s\n' % response.url
            content += 'Error detail: Missing some information'
            logging.error(content)
            return True
        return False

    def handle_downloader_error(self, response):
        """
        Returns:
            True if error exsits, False if no error
        """
        if response.error_code:
            logging.warn("downloader error: %s %s" %
                         (response.error_code, response.error_msg))
            self.request_queue.error_push_cache(
                Request(**json.loads(response.http_request))
            )
            return True
        return False

    def handle_status_error(self, response):
        """
        Returns:
            True if error exsits, False if no error
        """
        if response.status_code != 200:
            logging.warn("handle_status_error: %s" % response.status_code)
            self.handle_err(response)
            return True
        return False

    def handle_err(self, response):
        self.response = response
        func = self.handle_func_map.get(str(response.status_code), None)
        if not func:
            logging.warn('Not handle this status code: %d' %
                         response.status_code)
            self._handle_unknown_code()
            return
        return func()

    def _handle_404(self):
        flag = False
        for _404_string in self.html_404_strings:
            if all(map(lambda x: x in self.response.html, _404_string)):
                flag = True
                break
        if flag:    # a right 404 page, not because of proxy
            logging.info("404 page found")
        else:
            self.request_queue.error_push_cache(
                Request(**json.loads(self.response.http_request))
            )
        return flag

    # TODO(daihulin): handle these status codes
    def _handle_302(self):
        self.request_queue.error_push_cache(
            Request(**json.loads(self.response.http_request))
        )

    def _handle_301(self):
        self.request_queue.error_push_cache(
            Request(**json.loads(self.response.http_request))
        )

    def _handle_403(self):
        self.request_queue.error_push_cache(
            Request(**json.loads(self.response.http_request))
        )

    def _handle_unknown_code(self):
        self.request_queue.error_push_cache(
            Request(**json.loads(self.response.http_request))
        )
