#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*
import redis
import argparse
from yascrapy.ssdb import get_client
from yascrapy.ssdb import get_clients
from yascrapy.config import Config


def get(args):
    k = args.key
    if k == "":
        print "key can not be empty"
        return
    nodes = Config().get()["SSDBNodes"]
    clients = get_clients(nodes=nodes)
    client = get_client(clients, k)
    r = redis.Redis(connection_pool=client["connection_pool"])
    resp = r.get(k)
    print resp
    print "client: %s:%s" % (client["node"]["Host"], client["node"]["Port"])


def status(args):
    crawler_name = args.crawler_name
    if crawler_name == "":
        print "crawler_name can not be empty"
        return
    print "show %s crawler http_response status..." % crawler_name
    start = "http_response:%s:" % crawler_name
    end = "http_response:%s:z" % crawler_name
    nodes = Config().get()["SSDBNodes"]
    clients = get_clients(nodes=nodes)
    total = 0
    for client in clients:
        print "%s:%s" % (client["node"]["Host"], client["node"]["Port"])
        r = redis.Redis(connection_pool=client["connection_pool"])
        keys = r.execute_command("keys", start, end, -1)
        total += len(keys)
        print "length: ", len(keys)
    print "total: ", total
    



def input_params():
    parser = argparse.ArgumentParser(prog="responseadmin", 
        description="Target: see http_response status with specific crawler quickly"
    )
    subparsers = parser.add_subparsers(help='subcommand help')

    status_parser = subparsers.add_parser(
        "status",
        help="see http_response status in ssdb {crawler_name} kv store"
    )
    status_parser.set_defaults(func=status)
    status_parser.add_argument("-c", "--crawler_name", help="specify crawler name, can not be empty", default="", type=str)

    get_parser = subparsers.add_parser(
        "get",
        help="get http_response by key in ssdb kv store"
    )
    get_parser.set_defaults(func=get)
    get_parser.add_argument("-k", "--key", help="specify key, can not be empty", default="", type=str)

    args = parser.parse_args()
    args.func(args)

def main():
    input_params()

if __name__ == "__main__":
    main()
