from __future__ import absolute_import

from kinto.core.cache import CacheBase
from kinto_redis.storage import wrap_redis_error, create_from_config
from kinto.core.utils import json


class Cache(CacheBase):
    """Cache backend implementation using Redis.

    Enable in configuration::

        kinto.cache_backend = kinto_redis.cache

    *(Optional)* Instance location URI can be customized::

        kinto.cache_url = redis://localhost:6379/1

    A threaded connection pool is enabled by default::

        kinto.cache_pool_size = 50

    If the database is used for multiple Kinto deployement cache, you
    may want to add a prefix to every key to avoid collision::

        kinto.cache_prefix = stack1_

    :noindex:

    """

    def __init__(self, client, *args, **kwargs):
        super(Cache, self).__init__(*args, **kwargs)
        self._client = client

    @property
    def settings(self):
        return dict(self._client.connection_pool.connection_kwargs)

    def initialize_schema(self, dry_run=False):
        # Nothing to do.
        pass

    @wrap_redis_error
    def flush(self):
        self._client.flushdb()

    @wrap_redis_error
    def ttl(self, key):
        return self._client.ttl(self.prefix + key)

    @wrap_redis_error
    def expire(self, key, ttl):
        self._client.pexpire(self.prefix + key, int(ttl * 1000))

    @wrap_redis_error
    def set(self, key, value, ttl):
        if isinstance(value, bytes):
            raise TypeError("a string-like object is required, not 'bytes'")
        value = json.dumps(value)
        self._client.psetex(self.prefix + key, int(ttl * 1000), value)

    @wrap_redis_error
    def get(self, key):
        value = self._client.get(self.prefix + key)
        if value:
            value = value.decode('utf-8')
            return json.loads(value)

    @wrap_redis_error
    def delete(self, key):
        value = self.get(key)
        self._client.delete(self.prefix + key)
        return value


def load_from_config(config):
    settings = config.get_settings()
    client = create_from_config(config, prefix='cache_')
    return Cache(client, cache_prefix=settings['cache_prefix'])
