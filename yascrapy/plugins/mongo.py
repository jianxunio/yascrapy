#!/usr/bin/python
# coding: utf-8

import pymongo


class Plugin(object):

    def __init__(self, worker):
        db_name = worker.db_name
        columns = worker.mongo_tables
        self.client = pymongo.MongoClient(
            host=worker.mongo_ip,
            port=worker.mongo_port,
            connect=False,
            maxPoolSize=1000,
            w=0
        )
        self.db_name = db_name
        self.columns = columns
        self.is_set_index = False
        self.name = "db_handler"

    def _set_index(self):
        for column in self.columns:
            if column['index']:
                self.client[self.db_name][
                    column['name']].ensure_index(column['index'])

    def close(self):
        self.client.close()

    def exist(self, item):
        for column in self.columns:
            if isinstance(item, column["type"]):
                cnt = self.client[self.db_name][column['name']].find(
                    {column["index"]: item[column["index"]]}, {"_id": 1}
                ).limit(1).count(with_limit_and_skip=True)
                if cnt > 0:
                    return True
                else:
                    return False

    def update(self, item):
        if not self.is_set_index:
            self._set_index()
            self.is_set_index = True
        if isinstance(item, list):
            for each in item:
                self.update(each)
        else:
            for column in self.columns:
                if isinstance(item, column['type']):
                    self.client[self.db_name][column['name']].update(
                        {column['index']: item[column['index']]}, {'$set': item}, upsert=True)
                    break
