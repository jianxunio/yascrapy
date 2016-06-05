#!/usr/bin/env python
#encoding: utf-8
import requests
import time
import json
from yascrapy.response_queue import Response

def main():
    req_d = { 
        "crawler_name": "github",
        "url": "", 
        "proxy_name": "http_china",
        "method": "GET",
        "headers": {}, 
        "files": None,
        "data": None,
        "params": {}, 
        "auth": None,
        "cookies": {}, 
        "hooks": None,
        "json": None,
        "timeout": 10, 
    }   
    resp_d = {
        "crawler_name": "github",
        "http_request": "",
        "error_code": 0,
        "error_msg": "",
        "status_code": None,
        "reason": "",
        "html": "",
        "cookies": {},
        "url": "",
        "headers": {},
        "encoding": None,
        "elapsed": None,
        "http_proxy": "127.0.0.1:8000"
    }

    url = "https://github.com/cphilo"
    resp = requests.get(url)
    if resp.status_code == 200:
        print "get response html ok"
    else:
        print "get response html fail"
        return
    resp_d["crawler_name"] = "github"
    resp_d["error_code"] = 0 
    resp_d["error_msg"] = ""
    resp_d["html"] = resp.content 
    resp_d["url"] = resp.url
    resp_d["status_code"] = resp.status_code
    resp_d["reason"] = resp.reason
    resp_d["headers"] = {}
    resp_d["http_request"] = json.dumps(req_d)
    for k,v in resp.headers.items():
        resp_d["headers"][k] = v
    cnt = 0
    resp_json = json.dumps(resp_d)
    start = time.time()
    for i in range(10000):
        r = Response()
        r.from_json(resp_json)
        cnt += 1
        if cnt % 100 == 0:
            print cnt
    end = time.time()
    print "start: ", start
    print "end: ", end
    print "speed: %f/s" % (cnt / (end-start))

if __name__ == "__main__":
    main()
