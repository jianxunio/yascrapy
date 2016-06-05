#!/usr/bin/env python
# coding=utf-8
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def readme():
    with open('README.rst') as f:
        return f.read()

setup(
    name='yascrapy',
    version='0.1',
    description='yet another scrapy framework',
    author='Cphilo',
    author_email='cphilo@huntcoder.com',
    url='https://github.com/jianxunio/yascrapy.git',
    packages=[
        'yascrapy',
        'yascrapy.libs',
        'yascrapy.plugins',
        'yascrapy.tests'
    ],
    install_requires=[
        "redis",
        "hash_ring",
        "pika",
        "requests==2.8.1",
        "lxml",
        "cssselect",
        "pymongo"
    ],
    package_data={'yascrapy.tests': ['test.html', '404_err.html']},
    test_suite='nose.collector',
    tests_require=['nose', 'nose-cover3'],
    scripts=["bin/yascrapy_producer",
               "bin/yascrapy_worker", "bin/yascrapy_cache"],
    include_package_data=True
)
