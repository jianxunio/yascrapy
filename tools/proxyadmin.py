#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*
import redis
import argparse
import json
from yascrapy.ssdb import get_proxy_client
from yascrapy.config import Config

def status(args):
    cfg = Config().get()
    status_str = ["Stopped", "Running"]
    client = get_proxy_client(cfg=cfg)
    r = redis.Redis(connection_pool=client["connection_pool"])
    keys = r.hkeys("proxy_config")
    for k in keys:
        content = r.hget("proxy_config", k)
        proxy_config = json.loads(content)
        proxy_name = proxy_config["Name"]
        status = status_str[proxy_config["Status"]]
        cnt = r.scard("http_proxy:%s" % proxy_name)
        print "%s[%s]: %s items" % (proxy_name, status, cnt)

def input_params():
    parser = argparse.ArgumentParser(prog="proxyconfig", 
        description="Target: see proxy status and delete proxy in ssdb quickly"
    )
    subparsers = parser.add_subparsers(help='subcommand help')

    status_parser = subparsers.add_parser(
        "status",
        help="see proxy status in ssdb {proxy_name} queue"
    )
    status_parser.set_defaults(func=status)

    args = parser.parse_args()
    args.func(args)

def main():
    input_params()

if __name__ == "__main__":
    main()
