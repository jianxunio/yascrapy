# -*- coding: utf-8 -*-
import pika
import redis
from yascrapy.ssdb import get_client, get_clients
from yascrapy.request_queue import Request
from yascrapy.config import Config
import argparse
import multiprocessing
import threading
import time
import traceback


class Rabbitmqtossdb:
    def __init__(self, cfg):
        self.from_queue = cfg["from_queue"]
        self.from_host = cfg["from_host"]
        self.from_port = cfg["from_port"]
        self.crawler = cfg["crawler"]
        self.credentials = pika.PlainCredentials(cfg["user"], cfg["password"])
        nodes = Config().get()["SSDBNodes"]
        self.ssdb_clients = get_clients(nodes=nodes)

    def start_consumer(self):
        print "[info] start_consumer..."
        while True:
            try:
                from_conn = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.from_host, port=self.from_port, credentials=self.credentials))
                from_channel = from_conn.channel()
                from_channel.queue_declare(queue=self.from_queue, durable=True)
                from_channel.basic_qos(prefetch_count=1)
                from_channel.basic_consume(
                    self.callback,
                    queue=self.from_queue,
                    no_ack=False
                )
                from_channel.start_consuming()
            except (KeyboardInterrupt, SystemExit):
                from_channel.stop_consuming()
                break
            except Exception, e:
                print "[error]", str(e)
                print "[error]", traceback.print_exc()

    def callback(self, ch, method, properties, body):
        req = Request()
        req.from_json(body)
        k = "http_request:%s:%s" % (self.crawler, req.url)
        # print k
        client = get_client(self.ssdb_clients, k)
        # print client["tag"]
        rclient = redis.Redis(connection_pool=client['connection_pool'])
        rclient.set(k, body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

def input_params():
    parser = argparse.ArgumentParser(prog="rabbitmq queue migrate", 
        description="Target: move one queue on rabbitmq to another queue on rabbitmq"
    )
    parser.add_argument("-c", "--process_count", help="specify process number to run, default 1", type=int, default=1)
    parser.add_argument("-t", "--thread_count", help="specify thread number to run, default 1", type=int, default=1)
    parser.add_argument("--from_queue", help="specify from queue name on rabbitmq", default="", type=str)
    parser.add_argument("--from_host", help="specify from queue host name on rabbitmq, default 127.0.0.1", type=str, default="127.0.0.1")
    parser.add_argument("--from_port", help="specify from queue host name on rabbitmq, default 5673", type=int, default=5673)
    parser.add_argument("--crawler", help="specify crawler name", type=str, default="")
    parser.add_argument("--user", help="specify rabbitmq user, default guest", type=str, default="guest")
    parser.add_argument("--password", help="specify rabbitmq server password, default guest", type=str, default="guest")
    args = parser.parse_args()
    if args.crawler == "":
        print "[error] crawler can not be empty"
        return None
    if args.from_queue == "":
        print "[error] from_queue can not be empty"
        return None
    return args

def run_process(thread_count, cfg):
    threads = []
    for i in xrange(thread_count):
        m = Rabbitmqtossdb(cfg)
        t = threading.Thread(target=m.start_consumer) 
        t.daemon = True
        t.start()
        threads.append(t)
    while threading.active_count() > 0:
        time.sleep(0.1)

def main():
    args = input_params()
    if args is None:
        return
    process_list = []
    process_count = args.process_count
    cfg = {
        "from_queue": args.from_queue,
        "from_host": args.from_host,
        "from_port": args.from_port,
        "crawler": args.crawler,
        "user": args.user,
        "password": args.password
    }
    for i in range(process_count):
        process = multiprocessing.Process(target=run_process, args=(args.thread_count, cfg))
        process_list.append(process)
        process.start()
    for p in process_list:
        p.join()

if __name__ == "__main__":
    main()


