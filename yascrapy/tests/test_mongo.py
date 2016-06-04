# coding: utf-8
import unittest
from yascrapy.plugins.mongo import Plugin as MongoHandler

class TestMongo(unittest.TestCase):
    def setUp(self):
        pass

    def test_mongo(self):
        item = {'id': "adfkfdk", 'url': 'http://www.stackoverflow.com/xxxxxxxxxx'}
        db_name = 'test'
        columns = [
            {'name': 'users', 'index': 'id', 'type': dict}
        ]
        fake_worker = type("Worker", (object, ), {})
        setattr(fake_worker, "db_name", db_name)
        setattr(fake_worker, "mongo_tables", columns)
        setattr(fake_worker, "mongo_ip", "127.0.0.1")
        setattr(fake_worker, "mongo_port", 27017)
        handler = MongoHandler(fake_worker)
        handler.update(item)
        self.assertTrue(handler.exist(item))
        item["id"] = "$$"
        self.assertFalse(handler.exist(item))
        handler.close()

if __name__ == "__main__":
    unittest.main()
    

