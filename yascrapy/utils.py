#!/usr/bin/python
# coding: utf-8
import io
import base64
from requests.packages.urllib3 import HTTPResponse
import chardet


def init_req_data(crawler_name):
    return {
        "crawler_name": crawler_name,
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


def init_resp_data(crawler_name):
    return {
        "crawler_name": crawler_name,
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


def body_io(string, encoding=None):
    if hasattr(string, 'encode'):
        string = string.encode(encoding or 'utf-8')
    return io.BytesIO(string)


def add_urllib3_response(serialized, response, headers):
    if 'base64_string' in serialized['body']:
        body = io.BytesIO(
            base64.b64decode(serialized['body']['base64_string'].encode())
        )
    else:
        body = body_io(**serialized['body'])

    h = HTTPResponse(
        body,
        status=response.status_code,
        headers=headers,
        preload_content=False,
    )
    # NOTE(sigmavirus24):
    # urllib3 updated it's chunked encoding handling which breaks on recorded
    # responses. Since a recorded response cannot be streamed appropriately
    # for this handling to work, we can preserve the integrity of the data in
    # the response by forcing the chunked attribute to always be False.
    # This isn't pretty, but it is much better than munging a response.
    h.chunked = False
    response.raw = h


def decode(s):
    raw_s = s
    if hasattr(s, "decode"):
        try:
            raw_s = s.decode("ascii")
        except Exception:
            encoding = chardet.detect(s)["encoding"]
            raw_s = s.decode(encoding)
    return raw_s
