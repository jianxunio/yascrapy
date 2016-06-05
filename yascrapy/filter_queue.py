# -*- coding: utf-8 -*-


class FilterError(Exception):

    """This exception is raised when using `FilterQueue` class."""

    def __init__(self, value):
        """Use string `value` as error message."""
        self.value = value

    def __str__(self):
        return repr(self.value)


class FilterQueue(object):

    """We use this class to define interface to check where url is
        crawled or not. We use bloomd server on backend currently.
    """

    def __init__(self, bloomd_client=None, crawler_name=None, capacity=1e8, prob=1e-5):
        """Set filter queue initial params.

        :param bloomd_client: BloomdClient object, get it from `yascrapy.bloomd` module.
        :param crawler_name: string, crawler name.
        :param capacity: float, crawler links capacity, ensure that it is large enough.
        :param prob: float, error rate with crawler link checks.
        :raises: FilterError.

        create_filter will not update the existed bloomd_filter attributes.

        """
        if bloomd_client is None:
            raise FilterError("bloomd_client cannot be None")
        if crawler_name is None:
            raise FilterError("crawler_name cannot be None")
        self.bloomd_client = bloomd_client
        self.filter = self.bloomd_client.create_filter(
            crawler_name,
            capacity=capacity,
            prob=prob
        )

    def push(self, url):
        """Push url to this bloomd filter."""
        self.filter.add(url)

    def is_member(self, url):
        """Check whether url is crawled or not.

        :param url: string, url to be checked.
        :returns: bool, True if url if crawled.

        """
        if url in self.filter:
            return True
        return False
