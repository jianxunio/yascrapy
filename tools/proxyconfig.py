#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*
import argparse
import redis
import os
import json
from yascrapy.ssdb import get_proxy_client
from yascrapy.config import Config

def get_proxy_json():
    proxy_file = os.path.join("/etc/yascrapy", "proxy.json")
    with open(proxy_file, "r") as f:
        content = f.read()
    data = json.loads(content)
    return data["Configs"]



def load(args):
    cfg = Config().get()
    client = get_proxy_client(cfg=cfg)
    r = redis.Redis(connection_pool=client["connection_pool"])
    print "load proxy_config ..."
    keys = r.hkeys("proxy_config")
    for key in keys:
        r.hdel("proxy_config", key)
        print "delete proxyconfig by key: %s" % key
    for data in get_proxy_json():
        print "Add proxy_config %s" % data["Name"]
        r.hset("proxy_config", data["Name"], json.dumps(data))
    new_keys = r.hkeys("proxy_config")
    print "load ok keys: %s" % ",".join(new_keys)

def stop(args):
    cfg = Config().get()
    client = get_proxy_client(cfg=cfg)
    r = redis.Redis(connection_pool=client["connection_pool"])
    print "stop proxy_config %s..." % args.proxy
    keys = []
    if args.proxy:
        key = r.hget("proxy_config", args.proxy)
        if key:
            keys.append(args.proxy)
        else:
            print "%s not found " % args.proxy
    else:
        keys = r.hkeys("proxy_config")
    for key in keys:
        print "stop %s" % key
        proxy_config = json.loads(r.hget("proxy_config", key))
        proxy_config["Status"] = 0
        r.hset("proxy_config", key, json.dumps(proxy_config))

def delete(args):
    cfg = Config().get()
    client = get_proxy_client(cfg=cfg)
    r = redis.Redis(connection_pool=client["connection_pool"])
    print "delete proxy_config %s ..." % args.proxy
    keys = []
    if args.proxy:
        key = r.hget("proxy_config", args.proxy)
        if key:
            keys.append(args.proxy)
        else:
            print "%s not found " % args.proxy
    else:
        keys = r.hkeys("proxy_config")
    for key in keys:
        r.hdel("proxy_config", key)
        print "delete proxy_config %s ok" % key

def status(args):
    cfg = Config().get()
    client = get_proxy_client(cfg=cfg)
    r = redis.Redis(connection_pool=client["connection_pool"])
    status_str = ["Stopped", "Running"]
    keys = r.hkeys("proxy_config")
    for k in keys:
        content = r.hget("proxy_config", k)
        proxy_config = json.loads(content)
        status = status_str[proxy_config["Status"]]
        print "proxy_config %s Status: %s" % (proxy_config["Name"], status)

def input_params():
    parser = argparse.ArgumentParser(prog="proxyconfig", 
        description="Target: update proxy_config queue in ssdb quickly"
    )
    subparsers = parser.add_subparsers(help='subcommand help')

    stop_config_parser = subparsers.add_parser(
        'stop', 
        help='update proxy_config.Status to `StatusStopped` in ssdb proxy_config queue'
    )
    stop_config_parser.set_defaults(func=stop)
    stop_config_parser.add_argument('-p', '--proxy', help='specify proxy_config name, default delete all proxy_configs', default='', type=str)

    delete_config_parser = subparsers.add_parser(
        'delete', 
        help='delete proxy_config from ssdb proxy_config queue'
    )
    delete_config_parser.add_argument(
        '-p', 
        '--proxy', 
        help='specify proxy_config name, default delete all proxy_configs', 
        default='', 
        type=str
    )
    delete_config_parser.set_defaults(func=delete)

    load_config_parser = subparsers.add_parser(
        'load', 
        help="load proxy_config from `proxy.json` into ssdb proxy_config queue, clean previous proxy_configs"
    )
    load_config_parser.set_defaults(func=load)

    status_parser = subparsers.add_parser(
        'status', 
        help="see status of proxy_configs"
    )
    status_parser.set_defaults(func=status)

    args = parser.parse_args()
    args.func(args)

def main():
    input_params()

if __name__ == "__main__":
    main()
