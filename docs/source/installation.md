Installation
===

RabbitMQ
---
Check out the official site of RabbitMQ [here](https://www.rabbitmq.com/download.html).

You may need [HAProxy](http://www.haproxy.org/) for better performance.

The following is an example config for the HAProxy server that serves the RabbitMQ cluster of three nodes.

```config
global
    log         127.0.0.1 local0 info

    pidfile     /run/haproxy.pid
    maxconn     4096
    user        haproxy
    daemon

defaults
    mode                    tcp
    log                     global
    option                  tcplog
    option                  dontlognull
    option                  redispatch
    retries                 3
    timeout connect         5s
    timeout client          2m
    timeout server          2m
    maxconn                 2000

listen rabbitmq_local_cluster
    bind 127.0.0.1:5670
    mode tcp
    balance roundrobin
    server rabbit1 127.0.0.1:5673 check inter 5000 rise 2 fall 3
    server rabbit2 127.0.0.1:5674 check inter 5000 rise 2 fall 3
    server rabbit3 127.0.0.1:5675 check inter 5000 rise 2 fall 3

listen private_monitor
    bind 127.0.0.1:8100
    mode http
    option httplog
    stats enable
    stats uri /stats
    stats refresh 5s
```

Database
---
Yascrapy use [Redis](http://redis.io/), [SSDB](http://ssdb.io/) and [MongoDB](https://www.mongodb.com/) as the backend database.

The three are pretty easy to deploy, default configs are enough. Check [here](http://redis.io/download) for downloading Redis, [here](https://github.com/ideawu/ssdb) for installing SSDB and [here](https://www.mongodb.com/download-center#community) for MongoDB.

bloomd
---
Download and build from source.

```sh
$ git clone https://armon@github.com/armon/bloomd.git
$ cd bloomd
$ pip install SCons  # Uses the Scons build system, may not be necessary
$ scons
$ ./bloomd
```

Yascrapy
---
Download and install from source.

[comment]: <TODO> (change coding.net url to github url)

```sh
$ git clone https://git.coding.net/cphilo/yascrapy.git
$ python setup.py install
```

Now modify the config files(`core.json`, `proxy.json` and `common.json`) under the folder `conf` and fill up with the information of your own deployment.

`yascrapy_worker` is the script to start up `worker` component, `yascrapy_producer` is the producer to start up `producer` component, and `yascrapy_cache` is for handling cache.

Downloader component
---
Download and install from source.

[comment]: <TODO> (change coding.net url to github url)

```sh
$ go get git.coding.net/cphilo/downloader.git
```

Quick start
---
Yascrapy come with a Dockerfile that builds the environment of RabbitMQ, Redis, SSDB, MongoDB, and bloomd.

```sh
$ cd docker
$ docker build -t yascrapy .
$ docker run yascrapy
$ docker inspect CONTAINER_NAME | grep IPAddress  # get ip address of docker container
# edit the config file and try Yascrapy
```
