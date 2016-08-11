import mock

import redis
from pyramid import testing

from kinto.tests.core.support import unittest
from kinto.tests.core.test_permission import BaseTestPermission
from kinto_redis import permission as redis_backend


class RedisPermissionTest(BaseTestPermission, unittest.TestCase):
    backend = redis_backend
    settings = {
        'permission_url': '',
        'permission_pool_size': 10
    }

    def setUp(self):
        super(RedisPermissionTest, self).setUp()
        self.client_error_patcher = [
            mock.patch.object(
                self.permission._client,
                'execute_command',
                side_effect=redis.RedisError),
            mock.patch.object(
                self.permission._client,
                'pipeline',
                side_effect=redis.RedisError)]

    def test_config_is_taken_in_account(self):
        config = testing.setUp(settings=self.settings)
        config.add_settings({'permission_url': 'redis://:pass@db.loc:1234/5'})
        backend = self.backend.load_from_config(config)
        self.assertDictEqual(
            backend.settings,
            {'host': 'db.loc', 'password': 'pass', 'db': 5, 'port': 1234})

    def test_timeout_is_passed_to_redis_client(self):
        config = testing.setUp(settings=self.settings)
        config.add_settings({'permission_pool_timeout': '1.5'})
        backend = self.backend.load_from_config(config)
        self.assertEqual(backend._client.connection_pool.timeout, 1.5)
