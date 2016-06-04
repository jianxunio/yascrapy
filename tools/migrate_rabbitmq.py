# -*- coding: utf-8 -*-
import pika
import threading
import multiprocessing
import traceback
import time
import argparse
import os
import json

class Migration:
    def __init__(self, cfg):
        # self.queue_name = "http_request:weibo"
        self.from_queue = cfg["from_queue"]
        self.to_queue = cfg["to_queue"]
        self.from_host = cfg["from_host"]
        self.from_port = cfg["from_port"] 
        self.to_host = cfg["to_host"]
        self.to_port = cfg["to_port"]
        self.crawler_name = cfg["crawler"]
        self.credentials = pika.PlainCredentials(cfg["user"], cfg["password"])        
        self.routing_key = self.to_queue


    def start_consumer(self):
        print "[info] start_consumer..."
        while True:
            try:
                to_conn = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.to_host, port=self.to_port,
                        credentials=self.credentials
                    )
                )
                ch = to_conn.channel()
                ch.exchange_declare(
                    exchange=self.crawler_name, 
                    exchange_type="topic", 
                    durable=True
                )
                ch.close()
                ch = to_conn.channel()
                ch.queue_declare(queue=self.to_queue, durable=True)
                ch.queue_bind(exchange=self.crawler_name, queue=self.to_queue, routing_key=self.to_queue)
                ch.close()
                self.to_channel = to_conn.channel()
                # avoid losing message if connection closed
                # But this line can make publish slower
                # self.to_channel.confirm_delivery()

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
        # print "[info] consume", body
        try:
            ok = self.to_channel.basic_publish(
                exchange=self.crawler_name,
                routing_key=self.routing_key,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )
            if ok:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                print "[error] publish fail", body
        except Exception, e:
            raise

def input_params():
    parser = argparse.ArgumentParser(prog="rabbitmq queue migrate", 
        description="Target: move one queue on rabbitmq to another queue on rabbitmq"
    )
    parser.add_argument("-f", "--conf", help="specify migrate config file json, sample_config in `conf` dir", type=str)
    parser.add_argument("-c", "--process_count", help="specify process number to run", type=int, default=100)
    parser.add_argument("-t", "--thread_count", help="specify thread number to run", type=int, default=10)
    args = parser.parse_args()
    return args

def run_process(thread_count, migrate_cfg):
    threads = []
    for i in xrange(thread_count):
        m = Migration(migrate_cfg)
        t = threading.Thread(target=m.start_consumer) 
        t.daemon = True
        t.start()
        threads.append(t)
    while threading.active_count() > 0:
        time.sleep(0.1)

def main():
    args = input_params()
    conf_file = os.path.join('.', args.conf)
    with open(conf_file, "r") as f:
        migrate_cfg = json.loads(f.read())
    process_list = []
    process_count = args.process_count
    for i in range(process_count):
        process = multiprocessing.Process(target=run_process, args=(args.thread_count, migrate_cfg))
        process_list.append(process)
        process.start()
    for p in process_list:
        p.join()

if __name__ == "__main__":
    main()
