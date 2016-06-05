#!/usr/bin/python
# coding: utf-8

import pika
import logging
import time
import sys

host = '192.168.0.201'
port = 5673
queue = 'http_response:test_crawler'
logging.basicConfig(level=logging.INFO)

def test_rabbitmq_get():
    try:
        num = int(sys.argv[1])
    except:
        logging.error('arguments num is wrong, e.g: python test_rabbitmq 10000')
        return
    logging.info('start test basic_get')
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
    ch = conn.channel()
    ch.queue_declare(queue=queue,durable=True)
    ch.basic_qos(prefetch_count=1)
    start_time = time.time()
    cnt = 0
    while num:
        method, properties, body = ch.basic_get(queue=queue)
        if not method:
            break
        ch.basic_ack(method.delivery_tag)
        cnt += 1
        if cnt % 1000 == 0:
            logging.info(cnt)
        num -= 1
    end_time = time.time()
    logging.info("speed: %f" % (cnt / (end_time - start_time)))
    logging.info('end test basic_get')


def test_rabbitmq_consume():

    try:
        num = int(sys.argv[1])
    except:
        logging.error('arguments num is wrong, e.g: python test_rabbitmq 10000')
        return

    vars_ = {"cnt": 0, 'start_time': 0, 'end_time': 0}

    logging.info('start test basic_get')
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
    ch = conn.channel()
    ch.queue_declare(queue=queue,durable=True)
    ch.basic_qos(prefetch_count=1)

    vars_['start_time'] = time.time()

    def callback(ch, method, properties, body):
        ch.basic_ack(delivery_tag = method.delivery_tag)
        vars_['cnt'] += 1
        if vars_['cnt'] % 1000 == 0:
            logging.info(str(vars_['cnt']) + ' finish')
            vars_['end_time'] = time.time()
            logging.info("speed: %f" % (vars_['cnt'] / (vars_['end_time'] - vars_['start_time'])))
        if vars_['cnt'] >= num:
            raise Exception('finish')

    ch.basic_consume(callback,
                     queue=queue)
    ch.start_consuming()
    logging.info('end test basic_get')

if __name__ == '__main__':
    test_rabbitmq_consume()
    # test_rabbitmq_get()
