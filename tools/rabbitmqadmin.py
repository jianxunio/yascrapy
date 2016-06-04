#!/usr/bin/python
# coding: utf-8

import pika
import argparse
import os
import json
import random
import pprint
import traceback
import logging

def get_conn():
    conf_file = '/etc/yascrapy/config.json'
    if not os.path.exists(conf_file):
        print "%s not exist" % conf_file
        return None
    with open(conf_file, "r") as f:
        settings = json.loads(f.read())
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=settings["RabbitmqIp"], port=settings["RabbitmqPort"]))
    return conn

def delete(args):
    conn = get_conn()
    ch = conn.channel()
    queues = args.queues.split(',')
    for queue in queues:
        try:
            ch.queue_delete(queue=queue)
        except Exception as e:
            print e
            return
        print "delete queue %s" % queue
    conn.close()

def pop(args):
    conn = get_conn()
    ch = conn.channel()
    try:
        msg = ch.basic_get(queue=args.queue)
    except Exception as e:
        print e
        return
    pprint.pprint(json.loads(msg[2]))
    conn.close()

def get(args):
    cnt = 100
    conn = get_conn()
    ch = conn.channel()
    ch.basic_qos(prefetch_count=cnt)
    res = []
    while cnt:
        try:
            msg = ch.basic_get(queue=args.queue)
        except Exception as e:
            logging.error(traceback.print_exc())
            break
        try:
            res.append(json.loads(msg[2]))
        except:
            res.append(msg[2])
        cnt -= 1
    pprint.pprint(random.choice(res))
    conn.close()

def input_params():
    parser = argparse.ArgumentParser(prog="rabbitmq operators", 
        description="Target: rabbitmq operators, include delete, get, etc"
    )
    subparsers = parser.add_subparsers(help='subcommand help')

    delete_parser = subparsers.add_parser(
        "delete",
        help="rabbitmq queue delete operator"
    )
    delete_parser.add_argument(
        '-q', 
        '--queues', 
        help='specify the queues to be deleted, use "," to split queues, e.g queue1,queue2', 
        default='', 
        type=str
    )

    get_parser = subparsers.add_parser(
        "get",
        help="get one random message in rabbitmq queue"
    )

    get_parser.add_argument(
        '-q', 
        '--queue', 
        help='specify the queue name', 
        default='', 
        type=str
    )

    delete_parser.set_defaults(func=delete)
    get_parser.set_defaults(func=get)

    args = parser.parse_args()
    args.func(args)

def main():
    input_params()

if __name__ == "__main__":
    main()
