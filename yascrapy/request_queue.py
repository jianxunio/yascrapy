# -*- coding: utf-8 -*-
import json
from requests import Request as RequestLib
from .ssdb import get_client
import redis
import pika
import logging


class RequestError(Exception):

    """This exception is raised when using `Request` and `RequestQueue` class."""

    def __init__(self, value):
        """Use string `value` as error message."""
        self.value = value

    def __str__(self):
        return repr(self.value)


class Request(RequestLib):

    """`Request` class is used to store http request object.

    :Example usage::

        r = Request(
            method="GET",
            proxy_name="http_china",
            headers={},
            crawler_name="test_crawler",
            url="http://github.com",
        )

    """

    def __init__(self, method=None, url=None, proxy_name=None, headers=None, files=None,
                 data=None, params=None, auth=None, cookies=None, hooks=None,
                 crawler_name=None, timeout=None, json=None):
        '''This this the `Reqeust` class in ysacrapy.

        :param method: string, http method such as "GET", "POST", etc.
        :param url: string, http request url to use.
        :param proxy_name: string, get http proxy from proxy set using this proxy_name.
        :param headers: dict, http request headers.
        :param data: string, use data if `method` is "POST".
        :param cookies: dict, http request cookies.
        :param crawler_name: string, crawler name to use with this `Request` object.


        Use `requests.Request` as base class to keep interface simple. 
        Just use this class like `requests.Request`.

        '''

        RequestLib.__init__(
            self,
            method=method,
            url=url,
            headers=headers,
            files=files,
            data=data,
            params=params,
            auth=auth,
            cookies=cookies,
            hooks=hooks,
            json=json
        )
        self.data = data if data else ''
        self.crawler_name = crawler_name
        self.timeout = timeout
        self.proxy_name = proxy_name
        self.method = method if method else 'GET'
        self.timeout = timeout if timeout else 15

    def from_json(self, data):
        """Set `Request` attributes from data, no return value.

        :param data: json string to loads.
        :returns: no return value.

        """
        data = json.loads(data)
        attrs = ["proxy_name", "method", "url", "headers", "data",
                 "params", "cookies", "crawler_name", "timeout"]
        for attr in attrs:
            setattr(self, attr, data[attr])

    def to_json(self):
        """Convert `Request` to json string."""
        attrs = ["proxy_name", "method", "url", "headers", "data",
                 "params", "cookies", "crawler_name", "timeout"]
        d = {}
        for attr in attrs:
            v = getattr(self, attr)
            d[attr] = v
        return json.dumps(d)


class RequestQueue(object):

    """ `RequestQueue` is on rabbitmq server, contains `Request` message in json format.
    We use this class to get `Request` object and push `Request` object. In addition,
    We put `Request` object to ssdb as cache and use methods `safe_push_cache`, `push_cache`
    to operate on ssdb cache. The different between `push` and `safe_push` is that `safe_push`
    will check where this url is crawled or not, not push to the queue if crawled.

    :Example::

        from yascrapy.ssdb import get_clients
        from yascrapy.filter_queue import FilterQueue
        from yascrapy import bloomd
        from yascrapy.config import Config

        crawler_name = "test_crawler"
        cfg = Config(conf_file="/etc/yascrapy/common.json").get()
        bloomd_client = bloomd.get_client(nodes=cfg["BloomdNodes"])
        ssdb_clients = get_clients(nodes=cfg["SSDBNodes"])
        filter_q = FilterQueue(
            crawler_name=crawler_name,
            bloomd_client=bloomd_client
        )
        req_q = RequestQueue(
            crawler_name,
            ssdb_clients=ssdb_clients,
            filter_q=filter_q
        )

    """

    def __init__(self, crawler_name, ssdb_clients=None, filter_q=None, queue_name=None):
        """Rabbitmq-Server one physical queue on one node, use multiple queues with same crawler.

        :param queue_name: optional string, rabbitmq queue name,
            preferred format as `http_request:[crawler_name]:[number]`.
            default use `http_request:[crawler_name]`.
        :param crawler_name: crawler name to use.
        :param ssdb_clients: ssdb_clients to use, get this param from `yascrapy.ssdb` module.
        :param filter_q: `FilterQueue` object, get this param from `yascrap.filter_queue` module.
        :param queue_name: optional string, use default queue name if not specified.
        :raises: RequestError

        """
        if filter_q is None:
            raise RequestError("filter_q cannot be None")
        if ssdb_clients is None:
            raise RequestError("ssdb_clients can not be None")
        self.crawler_name = crawler_name
        self.ssdb_clients = ssdb_clients
        if queue_name is None:
            self.queue_name = 'http_request:%s' % crawler_name
            self.exchange_name = self.crawler_name
        else:
            self.queue_name = queue_name
            self.exchange_name = self.crawler_name
        self.error_queue_name = "http_request:%s:error" % crawler_name
        self.routing_key = self.queue_name
        self.filter_q = filter_q

    def declare_error_queue(self, channel):
        '''declare error queue, not used.'''
        self.declare(channel, queue_name=self.error_queue_name)

    def declare_queue(self, channel):
        '''Declare queue on rabbitmq server.

        :param channel: rabbitmq channel to use.

        Declare queue asynchronously.

        '''
        self.declare(channel)

    def declare(self, channel, queue_name=None):
        """Declare exchange, queue and queue bindings, use asynchronous rabbitmq connection.

        :param channel: rabbitmq channel to use.
        :param queue_name: optional string, use default queue name if not specified.

        """

        if queue_name is None:
            queue_name = self.queue_name
            routing_key = self.routing_key
        else:
            routing_key = queue_name

        def _on_queue_bind_ok(method):
            try:
                channel.close()
            except Exception as e:
                logging.error("channel close error: %s " % str(e))

        def _on_queue_declare_ok(method):
            try:
                channel.queue_bind(
                    _on_queue_bind_ok,
                    exchange=self.exchange_name,
                    queue=queue_name,
                    routing_key=routing_key
                )
            except Exception:
                logging.error("channel queue_bind error")

        def _on_exchange_declare_ok(method):
            try:
                channel.queue_declare(
                    _on_queue_declare_ok,
                    queue_name,
                    durable=True,
                    arguments={"x-max-length": 1000000}
                )
            except Exception as e:
                logging.error("channel queue_declare fail")

        try:
            channel.exchange_declare(
                callback=_on_exchange_declare_ok,
                exchange=self.exchange_name,
                durable=True,
                exchange_type="topic"
            )
        except Exception as e:
            logging.error("channel exchange_declare fail")

    def safe_push_cache(self, r, http_filter=None):
        """If `Request` url is not crawled, push request object to ssdb.

        :param r: `Request` object
        :param http_filter: optional string, use `r.url` on default if `http_filter` is not specified. 

        """
        if http_filter is None:
            http_filter = r.url
        is_crawled = self.filter_q.is_member(http_filter)
        if is_crawled:
            return
        else:
            self.push_cache(r)
            self.filter_q.push(http_filter)

    def push_cache(self, r):
        """Push request object to ssdb.

        :param r: `Reqeust` object
        :raises: `RequestError`

        """
        k = "http_request:%s:%s" % (r.crawler_name, r.url)
        client = get_client(self.ssdb_clients, k)
        if not client:
            raise RequestError('ssdb_client can not be none')
        rclient = redis.Redis(connection_pool=client['connection_pool'])
        rclient.set(k, r.to_json())

    def error_push_cache(self, r):
        '''interface for error handler to push `Request` to ssdb.

        :param r: `Request` object.

        '''
        self.push_cache(r)

    def error_push(self, r, channel):
        '''interface for error handler to push `Request` to rabbitmq.

        :param r: `Request` object to use.
        :param channel: rabbitmq channel to use.

        '''
        logging.debug("push to queue: %s" % self.error_queue_name)
        self.push(r, channel, self.error_queue_name)

    def push(self, r, channel, queue_name=None):
        """Push request object to rabbitmq server.

        :param r: `Request` object to use.
        :param channel: pusblish channel to use, you'd better consider using confirm delivery.
        :param queue_name: optional string, use default queue name if not specified.

        This function should not be used by workers and producers, use *safe_push* instead.

        """
        if r.url is None:
            raise RequestError("Request obj url cannot be None")
        if r.crawler_name is None:
            raise RequestError("Request obj crawler_name cannot be None")
        if r.crawler_name != self.crawler_name:
            raise RequestError(
                "Request obj crawler_name %s not equal queue crawler_name %s" %
                (r.crawler_name, self.crawler_name)
            )
        if queue_name is None:
            queue_name = self.queue_name
        if not isinstance(r, Request):
            raise RequestError("param must be Request object")
        # logging.info("push %s" % r.url)
        ok = channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=queue_name,
            body=r.to_json(),
            properties=pika.BasicProperties(
                delivery_mode=1
            )
        )
        return ok

    def safe_push(self, r, channel, http_filter=None):
        """If `Request` url is not crawled, push it to rabbitmq server.

        :param r: `Request` object.
        :param channel: rabbitmq channel to use.
        :param http_filter: optional string, use `r.url` on default if `http_filter` is not specified.

        If publish confirm fail, message lost and http_filter not set.
        """
        if http_filter is None:
            http_filter = r.url
        is_crawled = self.filter_q.is_member(http_filter)
        if is_crawled:
            return
        else:
            self.push(r, channel)
            self.filter_q.push(http_filter)
