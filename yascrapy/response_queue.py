#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import pika
import redis
from lxml.html import fromstring
import cssselect
from requests import Response as ResponseLib
from traceback import print_exc
from requests.packages.urllib3._collections import HTTPHeaderDict
from . import utils
from .ssdb import get_client
import logging


class ResponseError(Exception):

    """This exception is raised when using `Response` and `ResponseQueue` class."""

    def __init__(self, value):
        """Use string `value` as error message."""
        self.value = value

    def __str__(self):
        return repr(self.value)


class Response(ResponseLib):

    """Use `requests.Response` as the base class to make interface simple.

    This brings an issue yascrapy have requests libraray dependency. We have to
    use reqeusts.version `2.8.1` because we use some internal interface in `requests` 
    library.

    """

    def __init__(self):
        """Use response.text have performance problem, use response.html fix it.
        This is an issue, maybe some init charset not set, requests use chardet
        library to detect charset of the html content.

        """
        ResponseLib.__init__(self)

    def _set_raw(self):
        utils.add_urllib3_response({
            'body': {
                'string': self.html,
                'encoding': 'utf-8'
            }
        }, self, HTTPHeaderDict(self.headers))

    def from_json(self, data):
        """Set `Response` attributes from data, no return value.

        :param data: json string to loads.
        :returns: no return value.

        """
        data = json.loads(data)
        self.url = data["url"]
        self.html = data["html"]
        self.status_code = data["status_code"]
        self.reason = data["reason"]
        self.error_code = data["error_code"]
        self.error_msg = data["error_msg"]
        self.crawler_name = data["crawler_name"]
        self.http_request = data["http_request"]
        self.http_proxy = data["http_proxy"]
        self.root = None
        self._set_raw()

    def to_json(self):
        """Convert `Request` to json string."""
        attrs = ["url", "html", "status_code", "reason", "error_code",
                 "error_msg", "crawler_name", "http_request", "http_proxy"]
        d = {}
        for attr in attrs:
            v = getattr(self, attr)
            d[attr] = v
        return json.dumps(d)

    def _set_root(self):
        try:
            self.root = Selector(fromstring(self.html))
        except Exception as e:
            logging.error("fromstring error, html content: %s" % self.html)
            logging.info(print_exc())
            self.root = Selector(None)

    def xpath(self, xpath_selector):
        """To migrate crawlers from scrapy, must offer `Response.xapth` method.
        See source code if you want to see internal implementation, use `Selector` 
        and `SelectorList` class.
        """
        if not isinstance(self.root, Selector):
            self._set_root()
        return self.root.xpath(xpath_selector)

    def css(self, css_selector):
        """To migrate crawlers from scrapy, must offer `Response.css` method.
        See source code if you want to see internal implementation, use `Selector`
        and `SelectorList` class.
        """
        if not isinstance(self.root, Selector):
            self._set_root()
        return self.root.css(css_selector)


class SelectorList(list):

    def __init__(self, selectors):
        """
        Args:
            selectors: list of Selector object
        """
        self.selectors = selectors
        super(SelectorList, self).__init__(self.selectors)

    def css(self, css_selector):
        res = []
        for selector in self.selectors:
            res.extend(selector._css(css_selector))
        return SelectorList(res)

    def xpath(self, xpath_selector):
        res = []
        for selector in self.selectors:
            res.extend(selector._xpath(xpath_selector))
        return SelectorList(res)

    def extract(self):
        res = []
        for selector in self.selectors:
            if isinstance(selector.elem, str) or isinstance(selector.elem, unicode):
                res.append(selector.elem)
            else:
                res.append(selector.elem.text_content())
        return res


class Selector:

    def __init__(self, elem):
        """
        Args:
            elem: lxml.html.HtmlElement
        """
        self.elem = elem

    def strip(self):
        if isinstance(self.elem, str) or isinstance(self.elem, unicode):
            return self.elem.strip()
        return self.elem.text_content().strip()

    def css(self, css_selector):
        xpath_selector = cssselect.GenericTranslator().css_to_xpath(
            css_selector)
        return self.xpath(xpath_selector)

    def _css(self, css_selector):
        xpath_selector = cssselect.GenericTranslator().css_to_xpath(
            css_selector)
        return self._xpath(xpath_selector)

    def _xpath(self, xpath_selector):
        if self.elem is None:
            elems = []
        else:
            elems = self.elem.xpath(xpath_selector)
        return [Selector(elem) for elem in elems]

    def xpath(self, xpath_selector):
        return SelectorList(self._xpath(xpath_selector))


class ResponseQueue(object):

    """`ResponseQueue` have two types. `Response.url` queue is on rabbitmq server.
    `Response` json string is on ssdb. The `worker` get url from rabbitmq server and 
    get the response html content from ssdb indexed with the url."""

    def __init__(self, crawler_name, queue_name=None, ssdb_clients=None):
        """Set response queue init params.

        :param crawler_name: string, crawler name to use.
        :param queue_name: optional string, use `http_response:[crawler_name]` on default if not specified.
        :param ssdb_clients: ssdb clients, get it from `yascrapy.ssdb` module.
        :raises: ResponseError

        """
        self.crawler_name = crawler_name
        if queue_name is None:
            self.queue_name = "http_response:%s" % crawler_name
        else:
            self.queue_name = queue_name
        if ssdb_clients is None:
            raise ResponseError("ssdb_clients cannot be None")
        self.ssdb_clients = ssdb_clients

    def declare(self, channel):
        """Decalre queue asynchronously with the given rabbitmq channel."""
        def _on_queue_declare_ok(method):
            try:
                channel.close()
            except Exception as e:
                logging.error("channel close %s error" % self.queue_name)
        try:
            channel.queue_declare(
                _on_queue_declare_ok,
                queue=self.queue_name,
                durable=True,
                arguments={"x-max-length": 1000000}
            )
        except Exception as e:
            logging.error("channel queue_declare %s error" % self.queue_name)

    def push_cache(self, resp, resp_key):
        """Interface to push `Response` to ssdb.

        :param resp: `Response` object.
        :param resp_key: string.
        :raises: RespnseError.

        This function is used with test purpose.
        """
        client = get_client(self.ssdb_clients, resp_key)
        if not client:
            raise ResponseError('ssdb_client can not be none')
        r = redis.Redis(connection_pool=client['connection_pool'])
        r.set(resp_key, resp.to_json())

    def get(self, resp_key):
        """Get `Response` from ssdb indexed by the `resp_key`.

        :param resp_key: string.
        :returns: tuple, `(response, code)`. 
            `response` is `Response` object.
            `code`, 0 means OK, 1 means ssdb empty.
        """
        resp = Response()
        client = get_client(self.ssdb_clients, resp_key)
        r = redis.Redis(connection_pool=client["connection_pool"])
        resp_data = r.get(resp_key)
        r.delete(resp_key)
        if not resp_data:
            return None, 1
        resp.from_json(resp_data)
        return resp, 0

    def push(self, resp_key, channel):
        """Push response key to queue to rabbitmq server.

        :param resp_key: string, response key to use.
        :param channel: rabbitmq channel to use.

        `ResposneQueue` rabbitmq publish use default rabbitmq exchange, and use `queue_name`
        as the rabbitmq routing_key, `queue_name` is specified with initialization.

        """
        channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=resp_key,
            properties=pika.BasicProperties(
                delivery_mode=1
            )
        )
