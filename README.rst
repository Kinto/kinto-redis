Kinto Redis
############

|travis| |coveralls|

.. |travis| image:: https://travis-ci.org/Kinto/kinto-redis.svg?branch=master
    :target: https://travis-ci.org/Kinto/kinto-redis

.. |coveralls| image:: https://coveralls.io/repos/github/Kinto/kinto-redis/badge.svg?branch=master
    :target: https://coveralls.io/github/Kinto/kinto-redis?branch=master

**Kinto Redis** is a redis driver for `Kinto <https://kinto.readthedocs.io>`_
storage, permissions and cache backends.

.. note::

   The backend currently doesn't support transaction and will not work
   with plugins that are using the ResourceChanged event to stop the
   user action. i.e To validate the request or to handle quota management.

Installing ``kinto-redis``
==========================

You can use PyPI either installing kinto redis dependencies::

    pip install kinto[redis]

Or installing kinto-redis directly::

    pip install kinto-redis


Using Kinto Redis backends
==========================

After installing the ``kinto-redis`` package using PyPI, you can
configure your server like that::

    #
    # Backends.
    #
    # https://kinto.readthedocs.io/en/latest/configuration/settings.html#storage
    #
    kinto.storage_backend = kinto_redis.storage
    kinto.storage_url = redis://localhost:6379/1
    kinto.cache_backend = kinto_redis.cache
    kinto.cache_url = redis://localhost:6379/2
    kinto.permission_backend = kinto_redis.permission
    kinto.permission_url = redis://localhost:6379/3


Running the tests
=================

To run the unit tests::

  $ make tests
