# -*- coding: utf-8 -*-
import mock
import redis
from pyramid import testing

from kinto.core.storage import exceptions
from kinto.tests.core.test_storage import MemoryStorageTest, StorageTest
from kinto.tests.core.support import unittest

from kinto_redis import storage as redisbackend


class RedisStorageTest(MemoryStorageTest, unittest.TestCase):
    backend = redisbackend
    settings = {
        'storage_pool_size': 50,
        'storage_url': ''
    }

    def setUp(self):
        super(RedisStorageTest, self).setUp()
        self.client_error_patcher = mock.patch.object(
            self.storage._client.connection_pool,
            'get_connection',
            side_effect=redis.RedisError('connection error'))

    def test_config_is_taken_in_account(self):
        config = testing.setUp(settings=self.settings)
        config.add_settings({'storage_url': 'redis://:blah@store.loc:7777/6'})
        backend = self.backend.load_from_config(config)
        self.assertDictEqual(
            backend.settings,
            {'host': 'store.loc', 'password': 'blah', 'db': 6, 'port': 7777})

    def test_timeout_is_passed_to_redis_client(self):
        config = testing.setUp(settings=self.settings)
        config.add_settings({'storage_pool_timeout': '1.5'})
        backend = self.backend.load_from_config(config)
        self.assertEqual(backend._client.connection_pool.timeout, 1.5)

    def test_backend_error_provides_original_exception(self):
        StorageTest.test_backend_error_provides_original_exception(self)

    def test_raises_backend_error_if_error_occurs_on_client(self):
        StorageTest.test_raises_backend_error_if_error_occurs_on_client(self)

    def test_backend_error_is_raised_anywhere(self):
        with mock.patch.object(self.storage._client, 'pipeline',
                               side_effect=redis.RedisError):
            StorageTest.test_backend_error_is_raised_anywhere(self)

    def test_get_all_handle_expired_values(self):
        record = '{"id": "foo"}'.encode('utf-8')
        mocked_smember = mock.patch.object(self.storage._client, "smembers",
                                           return_value=['a', 'b'])
        mocked_mget = mock.patch.object(self.storage._client, "mget",
                                        return_value=[record, None])
        with mocked_smember:
            with mocked_mget:
                self.storage.get_all(**self.storage_kw)  # not raising

    def test_errors_logs_stack_trace(self):
        self.client_error_patcher.start()

        with mock.patch('kinto.core.storage.logger.exception') as exc_handler:
            with self.assertRaises(exceptions.BackendError):
                self.storage.get_all(**self.storage_kw)

        self.assertTrue(exc_handler.called)
