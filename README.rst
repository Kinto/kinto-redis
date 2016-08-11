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

Running the tests
=================

To run the unit tests::

  $ make tests
