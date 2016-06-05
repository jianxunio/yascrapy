#!/usr/bin/env python
# -*- coding: utf-8 -*
import redis
import hash_ring


def get_proxy_client(max_connections=100, cfg=None):
    """get proxy redis client.

    :param max_connections: optional int, connection pool size.
    :param cfg: `Config` object, get it from `yascrapy.config` module. 
    :returns: dict, contain `node`, `connection_pool` and `tag`. 

    """
    if cfg is None:
        raise Exception("cfg can't be None")
    node = {
        "Host": cfg["ProxyRedisIp"],
        "Port": cfg["ProxyRedisPort"]
    }
    conn_pool = redis.ConnectionPool(
        host=node["Host"], port=node["Port"], max_connections=max_connections, db=0)
    return {
        "node": node,
        "connection_pool": conn_pool,
        "tag": "%s:%s" % (node["Host"], node["Port"])
    }


def get_clients(max_connections=100, nodes=[]):
    """get multiple ssdb clients.

    :param max_connections: optional int, connection pool size.
    :returns: touple, `(clients, ring)`. `clients` is list of ssdb client, 
        each ssdb client is dict contains connection_pool and node info. 
        `ring` is hash ring object.

    """
    ssdb_nodes = []
    for node in nodes:
        conn_pool = redis.ConnectionPool(
            host=node["Host"], port=node["Port"], max_connections=max_connections, db=0)
        ssdb_nodes.append({
            "node": node,
            "connection_pool": conn_pool,
            "tag": "%s:%s" % (node["Host"], node["Port"])
        })
    servers = [node["tag"] for node in ssdb_nodes]
    # Init hash_ring is expensive operation, need reused.
    ring = hash_ring.HashRing(servers)
    return ssdb_nodes, ring


def get_client(clients, key):
    """get ssdb client by key, we use client side partitioning with ssdb cluster.

    :param clients: list, ssdb clients.
    :param key: string.
    :returns: ssdb client.

    """
    ssdb_nodes, ring = clients
    found_server = ring.get_node(key)
    if found_server is None:
        return None
    for ssdb_node in ssdb_nodes:
        if ssdb_node["tag"] == found_server:
            return ssdb_node
    return None
