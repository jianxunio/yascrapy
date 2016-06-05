#!/usr/bin/env python
# encoding: utf-8
from .request_queue import RequestQueue
from .response_queue import ResponseQueue
from .response_queue import Response
from .filter_queue import FilterQueue
from .ssdb import get_clients
from .ssdb import get_proxy_client
from .rabbitmq import create_conn
from .rabbitmq import AsyncConsumer
from . import bloomd
from .config import Config
from .utils import init_req_data, init_resp_data
import logging
import requests
import json
import random
import re
import inspect
import importlib


class BaseProducer(object):
    """Producer put initial links to http request queue on rabbitmq server.

    Example usage::

        from yascrapy.request_queue import Request
        from yascrapy.base import BaseProducer
        import random

        class Producer(BaseProducer):

            def run(self):
                urls = ["http://github.com", "http://baidu.com", "http://satckoverflow.com"]
                for url in urls:
                    r = Request(
                        url=url,
                        timeout=15,
                        crawler_name=self.crawler_name,
                        proxy_name=self.proxy_name,
                        method='GET',
                        params={},
                        data=''
                    )
                    q = random.choice(self.req_queues)
                    q.safe_push(r, self.publish_channel)

    """

    def __init__(self, producers=1, producer_index=1, settings=None, config_file="/etc/yascrapy/common.json"):
        """Init with params from `yascrapy_producer` script.

        :param config_file: optional string, config file used by all producers.
        :param  settings: python module object, configs used by this producer.

        """
        if settings is None:
            raise Exception("settings can not be None")
        self.producers = producers
        self.producer_index = producer_index
        self.load_settings(settings)
        cfg = Config(conf_file=config_file).get()
        self.ssdb_clients = get_clients(nodes=cfg["SSDBNodes"])
        self.rabbitmq_conn = create_conn(cfg)
        bloomd_client = bloomd.get_client(nodes=cfg["BloomdNodes"])
        filter_q = FilterQueue(
            crawler_name=self.crawler_name,
            bloomd_client=bloomd_client,
            capacity=self.bloomd_capacity,
            prob=self.bloomd_error_rate
        )

        ch = self.rabbitmq_conn.channel()
        ch.exchange_declare(
            exchange=self.crawler_name,
            exchange_type="topic",
            durable=True
        )
        ch.close()

        self.req_queues = []
        for i in xrange(self.request_queue_count):
            conn = create_conn(cfg)
            ch = conn.channel()
            if self.request_queue_count > 1:
                queue_name = "http_request:%s:%d" % (self.crawler_name, i)
            else:
                queue_name = "http_request:%s" % self.crawler_name
            ch.queue_declare(
                queue=queue_name,
                durable=True,
                arguments={"x-max-length": 1000000}
            )
            ch.queue_bind(
                exchange=self.crawler_name,
                queue=queue_name,
                routing_key=queue_name
            )
            ch.close()
            conn.close()
            q = RequestQueue(
                self.crawler_name,
                ssdb_clients=self.ssdb_clients,
                filter_q=filter_q,
                queue_name=queue_name
            )
            self.req_queues.append(q)

        self.publish_channel = self.rabbitmq_conn.channel()
        self.publish_channel.confirm_delivery()

    def load_settings(self, settings):
        """Load attributes from settings module.

        :param settings: python module object.

        Some builtin attributes is filtered by this method such as `__doc__`, `__name__`, etc.
        In addtion, if attibute is `class` object, this attribute is also filtered.

        """
        pattern = re.compile("__.+__")
        add_attributes = []
        for attr in dir(settings):
            if pattern.match(attr) is None:
                v = getattr(settings, attr)
                if not inspect.isclass(v):
                    setattr(self, attr, v)
                    add_attributes.append(attr)

    def run(self):
        """You need override this method to put initial links to `RequestQueue`."""
        pass


class BaseWorker(object):

    """This is the base worker class to be used by every crawler in yascrapy.

    This class is an interface for specific `worker` to inherit. If you need run
    `worker`, you need to switch specific `worker` directory::

        yascrapy_worker -n worker -c 5 -r 5 -f "/etc/yascrapy/common.json" &> worker.log

    Or if you just want to test whther your `worker` is ok::

        yascrapy_worker -n worker -t

    Just enojoy it!

    """

    def __init__(self, log_level=logging.INFO, test=False, profile=False,
                 profile_log="", config_file="/etc/yascrapy/common.json", settings=None):
        """Init with params from `yascrapy_worker` script.

        :param log_level: logging level to use, use `logging.INFO` on default.
        :param test: bool, use `False` on default.
        :param profile: bool, use `False` on default.
        :param profile_log: string, profile log file path if `profile` is `True`.
        :param config_file: optional string, config file used by all workers.
        :param  settings: python module object, configs used by this worker.

        """
        self.load_settings(settings)
        self.test_suffix = '__test'
        self.test = test
        if self.test:
            self.crawler_name += self.test_suffix
            self.bloomd_capacity = 1e5
            self.bloomd_error_rate = 1e-3
            self.response_queue_count = 1
            self.request_queue_count = 1
        self.profile = profile
        self.profile_log = profile_log
        self.cfg = Config(conf_file=config_file).get()
        self.ssdb_clients = get_clients(nodes=self.cfg["SSDBNodes"])
        self.proxy_client = get_proxy_client(cfg=self.cfg)
        self.bloomd_client = bloomd.get_client(nodes=self.cfg["BloomdNodes"])
        self.filter_q = FilterQueue(
            crawler_name=self.crawler_name,
            bloomd_client=self.bloomd_client,
            capacity=self.bloomd_capacity,
            prob=self.bloomd_error_rate
        )
        logging.basicConfig(
            level=log_level,
            format="[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)s:%(funcName)s] %(message)s",
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def load_settings(self, settings):
        """Load attributes from settings module.

        :param settings: python module object.

        Some builtin attributes is filtered by this method such as `__doc__`, `__name__`, etc.
        In addtion, if attibute is `class` object, this attribute is also filtered.

        """
        pattern = re.compile("__.+__")
        add_attributes = []
        for attr in dir(settings):
            if pattern.match(attr) is None:
                v = getattr(settings, attr)
                if not inspect.isclass(v):
                    setattr(self, attr, v)
                    add_attributes.append(attr)

    def load_plugins(self):
        """Load plugins used by this worker.

        All plugins get `self` as parameter. `self` have attributes as follows.

            * all attributes spcified in `settings` module.
            * publish_channel, rabbitmq channel to publish `Request` object.
            * ssdb_clients, ssdb clients to use.
            * proxy_client, proxy client to use.
            * filter_q, `FilterQueue` object to use.
            * req_q, `RequestQueue` object to use.
            * cfg, `Config` object to use.

        These attributes are offered to plugins. If you want to write your own plugins,
        see `yascrapy/plugins` to get some samples.

        """
        for plugin in self.plugins:
            plugin_module = importlib.import_module(plugin)
            plugin_instance = plugin_module.Plugin(self)
            setattr(self, plugin_instance.name, plugin_instance)

    def callback(self, channel, method, properties, body):
        """You have to write this method to parse data from rabbitmq and ssdb.

        :param channel: rabbitmq channel to use with `RequestQueue`.
        :param method: rabbitmq message method frame to use.
        :param properties: this param is related to rabbitmq message, just ignore it.
        :param body: response key to use.

        :Example usage::

            resp_key = body
            response, err = self.resp_q.get(resp_key)
            channel.basic_ack(delivery_tag=method.delivery_tag)

            ... Then comes to your response parsing logic ...

        """
        pass

    def _callback(self, channel, method, properties, body):
        logging.info(method.NAME)
        try:
            self.callback(channel, method, properties, body)
        except Exception, e:
            logging.error("callback catch exception: %s" % str(e))
            channel.close()

    def run(self):
        """Entry with `yascrapy_worker` script.

        Use asynchronous rabbitmq ioloop. If you want to see internal implementation,
        see `yascrapy.rabbitmq` module.

        """
        if self.test:
            self.set_test_env()
        try:
            consumer = AsyncConsumer(
                cfg=self.cfg, cbk=self._callback, worker=self)
            rabbitmq_conn = consumer.connect()
            rabbitmq_conn.ioloop.start()
        except (KeyboardInterrupt, SystemExit):
            consumer.stop()

    def init_resp_queue(self, rabbitmq_conn):
        '''called from AsyncConsumer.

        :param rabbitmq_conn: rabbitmq connection to use.

        '''
        if self.response_queue_count == 1:
            resp_queue_name = None
        else:
            resp_queue_index = random.randint(0, self.response_queue_count - 1)
            resp_queue_name = "http_response:%s:%d" % (
                self.crawler_name, resp_queue_index)
        logging.debug("init_resp_queue: %s" % resp_queue_name)
        self.resp_q = ResponseQueue(
            self.crawler_name,
            ssdb_clients=self.ssdb_clients,
            queue_name=resp_queue_name
        )
        rabbitmq_conn.channel(on_open_callback=self.resp_q.declare)

    def init_req_queue(self, rabbitmq_conn):
        """Called from AsyncConsumer.

        :param rabbitmq_conn: rabbitmq connection to use.

        Need set self.publish_channel before calling this func.
        """
        if self.request_queue_count == 1:
            req_queue_name = None
        else:
            request_queue_index = random.randint(
                0, self.request_queue_count - 1)
            req_queue_name = "http_request:%s:%d" % (
                self.crawler_name, request_queue_index)
        logging.debug("init_req_queue: %s" % req_queue_name)
        self.req_q = RequestQueue(
            self.crawler_name, ssdb_clients=self.ssdb_clients,
            filter_q=self.filter_q,
            queue_name=req_queue_name
        )
        rabbitmq_conn.channel(on_open_callback=self.req_q.declare_queue)

        self.load_plugins()

    def set_test_env(self):
        """Set up `worker` test environment. Request some test urls and put the response
        to rabbitmq and ssdb. Then `worker` will parse the response data to figure out whether
        it parses the test urls correctly. If you have more complicated testing environment,
        just override this function in your own `worker`.

        """
        test_urls = getattr(self, "test_urls", [])
        test_headers = getattr(self, "test_headers", {})
        if not test_urls:
            logging.error("test urls were not set in Worker class, exit")
        else:
            logging.info("test_urls %s " % ",".join(test_urls))

        req_d = init_req_data(self.crawler_name)
        resp_d = init_resp_data(self.crawler_name)

        resp_arr = []
        for url in test_urls:
            resp = requests.get(url, headers=test_headers)
            req_d = init_req_data(self.crawler_name)
            resp_d = init_resp_data(self.crawler_name)
            resp_d["html"] = resp.content
            resp_d["url"] = resp.url
            resp_d["status_code"] = resp.status_code
            resp_d["reason"] = resp.status_code
            resp_d["headers"] = {}
            req_d["url"] = url
            resp_d["http_request"] = json.dumps(req_d)
            for k, v in resp.headers.items():
                resp_d["headers"][k] = v
            resp_arr.append(json.dumps(resp_d))

        rabbitmq_conn = create_conn(self.cfg)
        ch = rabbitmq_conn.channel()
        resp_q = ResponseQueue(
            self.crawler_name,
            ssdb_clients=self.ssdb_clients
        )
        for resp_data in resp_arr:
            resp = Response()
            resp.from_json(resp_data)
            resp_key = 'http_response:%s:%s' % (self.crawler_name, resp.url)
            resp_q.push_cache(resp, resp_key)
            resp_q.push(resp_key, ch)
        ch.close()
        rabbitmq_conn.close()

    # def new_task(self, url, force_add=False):
    #     if url.startswith('http'):
    #         pass
    #     elif url.startswith('/'):
    #         url = self.domain + url
    #     else:
    #         url = '%s/%s' % (self.domain, url)
    #     if force_add:
    #         self.req_q.push(Request(
    #             url=url, crawler_name=self.crawler_name, proxy_name=self.proxy_name), self.publish_channel)
    #     else:
    #         self.req_q.safe_push(Request(
    # url=url, crawler_name=self.crawler_name, proxy_name=self.proxy_name),
    # self.publish_channel)
