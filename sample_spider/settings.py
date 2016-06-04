#!/usr/bin/env python
# encoding: utf-8
from items import TestItem

# settings that used by all crawlers
crawler_name = "sample_crawler"
proxy_name = "http_oversea"
bloomd_capacity = 100000
bloomd_error_rate = 1e-3
request_queue_count = 10
response_queue_count = 5

# settings used by this crawler
test_urls = [
    'http://stackoverflow.com/users/771848/alecxe',
    'http://stackoverflow.com/users/2698777/cphilo',
    'http://stackoverflow.com/users/341994/matt'
]
# all xpath strings
name_xpath = '//h2[@class="user-card-name"]/text()[1]'


plugins = [
    "yascrapy.plugins.mongo",
    "yascrapy.plugins.handle_error",
    "yascrapy.plugins.handle_proxy",
]

# mongo plugin settings
mongo_ip = "127.0.0.1"
mongo_port = 27017
mongo_tables = [
    {'name': 'users', 'index': 'uid', 'type': TestItem},
]
db_name = 'sample_crawler'

# handle_error plugin settings
html_404_strings = [["Page", "Not", "Found"]]
