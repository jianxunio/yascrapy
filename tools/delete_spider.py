# -*- coding: utf-8 -*-
import pika
import argparse
import redis
from yascrapy.config import Config
from yascrapy import bloomd
from yascrapy.rabbitmq import create_conn


def del_bloomd_filter(cfg, crawler_name):
    client = bloomd.get_client(nodes=cfg["BloomdNodes"])
    f = client.create_filter(crawler_name)
    try:
        f.drop()
        print "[info] drop bloomd %s success" % crawler_name
    except Exception:
        print "[error] drop bloomd %s fail" % crawler_name


def del_rabbitmq_queue(cfg, crawler_name, req_queue_count, resp_queue_count):
    connection = create_conn(cfg)
    channel = connection.channel()

    for i in range(req_queue_count):
        queue_name = "http_request:%s:%d" % (crawler_name, i)
        channel.queue_delete(queue=queue_name)
        print "[info] delete %s" % queue_name

    for i in range(resp_queue_count):
        queue_name = "http_response:%s:%s" % (crawler_name, i)
        channel.queue_delete(queue=queue_name)
        print "[info] delete %s" % queue_name

    connection.close()
    print "[info] delelte %s rabbitmq queues success" % crawler_name


def del_ssdb_cache(cfg, crawler_name):
    for node in cfg["SSDBNodes"]:
        ssdb_host = node["Host"]
        ssdb_port = node["Port"]
        conn_pool = redis.ConnectionPool(
            host=ssdb_host, port=ssdb_port, max_connections=100, db=0
        )
        r = redis.Redis(connection_pool=conn_pool)


        start = "http_request:%s:" % crawler_name
        end = "http_request:%s:z" % crawler_name
        keys = r.execute_command("keys", start, end, -1)
        print "[info] crawler %s req keys: %d" % (crawler_name, len(keys))
        for k in keys:
            r.delete(k)
        print "[info] del crawler %s req keys on %s:%s success" % (crawler_name, ssdb_host, ssdb_port) 
        
        start = "http_response:%s:" % crawler_name
        end = "http_response:%s:z" % crawler_name
        keys = r.execute_command("keys", start, end, -1)
        print "[info] crawler %s resp keys: %d" % (crawler_name, len(keys))
        for k in keys:
            r.delete(k)
        print "[info] del crawler %s resp keys on %s:%s success" % (crawler_name, ssdb_host, ssdb_port) 


def input_params():
    parser = argparse.ArgumentParser(
        prog="delete spider rabbitmq queues and bloomd filter",
        description="Target: delete spider quickly"
    )
    parser.add_argument(
        "-f",
        "--conf",
        help="specify config file, default `/etc/yascrapy/common.json`",
        type=str, default="/etc/yascrapy/common.json")
    parser.add_argument(
        "-n",
        "--crawler_name",
        help="specify crawler name",
        type=str
    )
    parser.add_argument(
        "--req",
        help="specify crawler request queue count",
        type=int,
        default=1
    )
    parser.add_argument(
        "--resp",
        help="specify crawler response queue count",
        type=int,
        default=1
    )
    args = parser.parse_args()
    return args


def main():
    args = input_params()
    cfg = Config(conf_file=args.conf).get()
    del_bloomd_filter(cfg, args.crawler_name)
    del_rabbitmq_queue(cfg, args.crawler_name, args.req, args.resp)
    del_ssdb_cache(cfg, args.crawler_name)

if __name__ == "__main__":
    main()
