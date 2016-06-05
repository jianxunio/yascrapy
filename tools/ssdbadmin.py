import redis
import argparse
import os
import json
import random
import pprint

def get_redis():
    redis_clis = []
    conf_file = '/etc/yascrapy/common.json'
    if not os.path.exists(conf_file):
        print "%s not exist" % conf_file
        return []
    with open(conf_file, "r") as f:
        ssdb_nodes = json.loads(f.read())["SSDBNodes"]
    for node in ssdb_nodes:
        try:
            cli = redis.Redis(host=node["Host"], port=node["Port"])
        except Exception as e:
            print e
            continue
        redis_clis.append(cli)
    return redis_clis

def delete(args):
    redis_clis = get_redis()
    cnt = 0
    for cli in redis_clis:
        keys = cli.execute_command("keys", args.prefix, "%szzzzzzzz" % args.prefix, "-1")
        map(lambda x: cli.delete(x), keys)
        cnt += len(keys)
        cli.delete(args.prefix)
        cnt += 1
    print 'delete %d keys' % cnt

def flushdb(args):
    redis_clis = get_redis()
    for cli in redis_clis:
        cli.flushdb()
    print 'flushdb done'

def get(args):
    clis = get_redis()
    for cli in clis:
        content = cli.get(args.prefix)
        if content:
            pprint.pprint(json.loads(content))
            return content
        keys = clis[0].execute_command("keys", args.prefix, "%szzzzzzzzzzzzzzzzzzzzzzzzzzzzzz" % args.prefix, "100")
        if not keys:
            continue
        content = cli.get(random.choice(keys))
        pprint.pprint(json.loads(content))
        return
    print "no this key"

def input_params():
    parser = argparse.ArgumentParser(prog="ssdb operators", 
        description="Target: operate ssdb, include delete, get, etc"
    )
    subparsers = parser.add_subparsers(help='subcommand help')

    delete_parser = subparsers.add_parser(
        "delete",
        help="delete keys with prefix"
    )

    flushdb_parser = subparsers.add_parser(
        "flushdb",
        help="flush all db on every node"
    )

    get_parser = subparsers.add_parser(
        "get",
        help="get the value of key with prefix you given"
    )

    delete_parser.add_argument(
        '-p', 
        '--prefix', 
        help='keys with this prefix will be deleted', 
        default='', 
        type=str
    )

    get_parser.add_argument(
        '-p', 
        '--prefix', 
        help='a key with this prefix will be given', 
        default='', 
        type=str
    )

    delete_parser.set_defaults(func=delete)
    flushdb_parser.set_defaults(func=flushdb)
    get_parser.set_defaults(func=get)

    args = parser.parse_args()
    args.func(args)

def main():
    input_params()

if __name__ == "__main__":
    main()
