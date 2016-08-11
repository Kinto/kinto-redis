# -*- coding: utf-8 -*-
import mock
import redis
from pyramid import testing

from kinto.tests.core.support import unittest
from kinto.tests.core.test_cache import BaseTestCache
from kinto_redis import cache as redis_backend


class RedisCacheTest(BaseTestCache, unittest.TestCase):
    backend = redis_backend
    settings = {
        'cache_url': '',
        'cache_pool_size': 10,
        'cache_prefix': ''
    }

    def setUp(self):
        super(RedisCacheTest, self).setUp()
        self.client_error_patcher = mock.patch.object(
            self.cache._client,
            'execute_command',
            side_effect=redis.RedisError)

    def test_config_is_taken_in_account(self):
        config = testing.setUp(settings=self.settings)
        config.add_settings({'cache_url': 'redis://:secret@peer.loc:4444/7'})
        backend = self.backend.load_from_config(config)
        self.assertDictEqual(
            backend.settings,
            {'host': 'peer.loc', 'password': 'secret', 'db': 7, 'port': 4444})

    def test_timeout_is_passed_to_redis_client(self):
        config = testing.setUp(settings=self.settings)
        config.add_settings({'cache_pool_timeout': '1.5'})
        backend = self.backend.load_from_config(config)
        self.assertEqual(backend._client.connection_pool.timeout, 1.5)
