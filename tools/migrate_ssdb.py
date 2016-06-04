# -*- coding: utf-8 -*-
import argparse
import os
import json
import redis
import time
import multiprocessing


class SSDBMigration:
    def __init__(self, cfg):
        print cfg
        self.crawler_name = cfg["crawler"]
        self.from_host = cfg["from_host"]
        self.from_port = cfg["from_port"]
        self.to_host = cfg["to_host"]
        self.to_port = cfg["to_port"]

    def run(self):
        start = "http_request:%s:" % self.crawler_name
        end = "http_request:%s:z" % self.crawler_name
        from_conn_pool = redis.ConnectionPool(
            host=self.from_host,
            port=self.from_port,
            max_connections=100,
            db=0
        )
        to_conn_pool = redis.ConnectionPool(
            host=self.to_host,
            port=self.to_port,
            max_connections=100,
            db=0
        )
        fclient = redis.Redis(connection_pool=from_conn_pool)
        tclient = redis.Redis(connection_pool=to_conn_pool)
        cnt = 0
        while True:
            if cnt % 10000 == 0:
                print cnt
            keys = fclient.execute_command("keys", start, end, 1000)
            if len(keys) == 0:
                print "[info] ssdb node %s:%s empty" % (self.from_host, self.from_port)
                time.sleep(5)
                continue
            values = fclient.mget(keys)
            d = {k: values[i] for i, k in enumerate(keys)}
            for k, v in d.items():
                # print "[info] get k", k
                pass
            tclient.mset(d)
            fclient.delete(*keys)
            cnt += 1000


def input_params():
    parser = argparse.ArgumentParser(prog="ssdb queue migrate",
                                     description="Target: move one queue on ssdb to another queue on ssdb"
                                     )
    parser.add_argument(
        "-f", "--conf", help="specify migrate config file json, sample_config in `conf` dir", type=str)
    args = parser.parse_args()
    return args


def main():
    args = input_params()
    conf_file = os.path.join('.', args.conf)
    with open(conf_file, "r") as f:
        migrate_cfg = json.loads(f.read())
    process_list = []
    for cfg in migrate_cfg:
        m = SSDBMigration(cfg)
        process = multiprocessing.Process(target=m.run)
        process_list.append(process)
        process.start()

if __name__ == "__main__":
    main()
