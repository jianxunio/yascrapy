# -*- coding: utf-8 -*
from .libs.pybloomd import BloomdClient


def get_client(nodes=[]):
    """Get bloomd client, note that this bloomd client is not thread-safe.

    :param nodes: bloomd nodes from config file.
    :returns: `BoomdClient` object.

    """
    tags = []
    for node in nodes:
        tags.append("%s:%s" % (node["Host"], node["Port"]))
    return BloomdClient(tags)
