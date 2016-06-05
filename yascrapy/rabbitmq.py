#!/usr/bin/python
# -*- coding: utf-8 -*-
import pika
import logging
import time


def create_conn(cfg):
    """Returns rabbitmq blocking connection and simplify rabbitmq usage.

    :param cfg: `Config` object, get it from `yascrapy.Config` module.
    :returns: `pika.BlockingConnection`.
    
    """

    credentials = pika.PlainCredentials(
        cfg["RabbitmqUser"], cfg["RabbitmqPassword"])
    conn = pika.BlockingConnection(pika.ConnectionParameters(
        connection_attempts=5,
        heartbeat_interval=25,
        socket_timeout=10,
        host=cfg["RabbitmqIp"],
        port=cfg["RabbitmqPort"],
        credentials=credentials
    ))
    return conn


class AsyncConsumer:

    """Define asynchronous ioloop to fetch links from rabbitmq `http_request` queue.

    This class is for internal use.

    """

    def __init__(self, cbk=None, cfg=None, worker=None):
        if cfg is None:
            raise Exception("please specify cfg")
        if cbk is None:
            raise Exception("please specify cbk func")
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._on_message = cbk
        self.cfg = cfg
        self._url = "%s:%s" % (
            self.cfg["RabbitmqIp"], self.cfg["RabbitmqPort"])

        # worker is `yascraoy.Worker` instance
        self.worker = worker

    def get_params(self):
        credentials = pika.PlainCredentials(
            self.cfg["RabbitmqUser"], self.cfg["RabbitmqPassword"])
        return pika.ConnectionParameters(
            self.cfg["RabbitmqIp"],
            self.cfg["RabbitmqPort"],
            '/',
            credentials,
            connection_attempts=5,
            heartbeat_interval=0,
        )

    def on_channel_closed(self, channel, reply_code, reply_text):
        logging.info("channel  closed: (%s) %s" % (reply_code, reply_text))
        self.stop()
        self.start()

    def on_consumer_cancelled(self, method_frame):
        logging.info(
            'Consumer was cancelled remotely, shutting down: %r', method_frame)
        self.stop()
        self.start()

    def on_channel_open(self, channel):
        logging.info("channel opened")
        self.worker.init_resp_queue(self._connection)
        self._channel = channel
        self._channel.basic_qos(prefetch_count=1)
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._channel.add_on_close_callback(self.on_channel_closed)
        logging.info("add some yascrapy callbacks to consumer channel")
        self._consumer_tag = self._channel.basic_consume(
            self._on_message,
            self.worker.resp_q.queue_name,
        )
        logging.info("start consuming ok")
        logging.info(self._channel._state)

    def on_connection_closed(self, connection, reply_code, reply_text):
        logging.info("connection closed: (%s) %s" % (reply_code, reply_text))
        self.stop()
        self.start()

    def on_publish_channel_closed(self, channel, reply_code, reply_text):
        logging.info("worker publish channel closed: (%s) %s" %
                     (reply_code, reply_text))
        self.stop()
        self.start()

    def on_delivery_confirm(self, method_frame):
        # logging.info("coming to on_delivery_confirm")
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        if confirmation_type == "nack":
            self.worker.publish_channel.basic_reject(
                method_frame.method.delivery_tag,
                requeue=True
            )

    def start(self):
        logging.info("consumer start")
        self._connection = self.connect()
        try:
            self._connection.ioloop.start()
        except Exception as e:
            logging.error("start connetion.ioloop error: %s" % str(e))
            logging.info("restart after 1s...")
            time.sleep(1)
            self.stop()
            self.start()

    def on_publish_channel_open(self, publish_channel):
        logging.info("publish_channel opened")
        self.worker.publish_channel = publish_channel
        publish_channel.confirm_delivery(self.on_delivery_confirm)
        publish_channel.add_on_close_callback(self.on_publish_channel_closed)
        self.worker.init_req_queue(self._connection)

    def on_connection_open(self, conn):
        logging.info("connect to rabbitmq success")
        self._connection = conn
        self._connection.add_on_close_callback(self.on_connection_closed)
        self._connection.channel(on_open_callback=self.on_channel_open)
        self._connection.channel(on_open_callback=self.on_publish_channel_open)

    def on_cancelok(self, frame):
        self._channel.close()

    def stop(self):
        try:
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)
            logging.info('Sending a Basic.Cancel RPC command to RabbitMQ')
        except Exception as e:
            logging.warn("Sending a Basic.Cancel fail: %s" % str(e))
        logging.info("consumer stoppped")
        try:
            self._connection.ioloop.stop()
        except Exception as e:
            logging.eror("ioloop stop error: %s" % str(e))
        try:
            self._connection.close()
        except Exception as e:
            logging.error("rabbitmq connection closed: %s" % str(e))

    def on_connection_error(self, conn, error):
        logging.error("connection error, retry after 1s...")
        time.sleep(1)
        self.start()

    def connect(self):
        logging.info("start connect to rabbitmq...")
        return pika.SelectConnection(
            parameters=self.get_params(),
            on_open_error_callback=self.on_connection_error,
            on_open_callback=self.on_connection_open,
            stop_ioloop_on_close=False
        )
