# -*- coding: utf-8 -*-
import json
import os


class Config(object):

    """Get Config object from specified config file.

    :Sample config file::

        {
            "RabbitmqIp": "127.0.0.1",
            "RabbitmqPort": 5672,
            "RabbitmqManagerIp": "127.0.0.1",
            "RabbitmqManagerPort": 15672,
            "RabbitmqUser": "guest",
            "RabbitmqPassword": "guest",
            "ProxyRedisIp": "127.0.0.1",
            "ProxyRedisPort": 6379,
            "SSDBNodes": [{
                "Host": "127.0.0.1",
                "Port": 8888
            }],
            "BloomdNodes": [{
                "Host": "127.0.0.1",
                "Port": 8673
            }]
        }   

    """

    def __init__(self, conf_file="/etc/yascrapy/common.json"):
        """Set config file to load.

        :param conf_file: optional string, specify config file path.

        """
        self.conf_file = conf_file

    def get(self):
        """Get `Config` object from config file.

        :returns: dict, `Config` object.

        """
        if not os.path.exists(self.conf_file):
            raise Exception("%s not exist" % self.conf_file)
        with open(self.conf_file, "r") as f:
            cnf = json.loads(f.read())
        return cnf
