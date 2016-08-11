# -*- coding: utf-8 -*-
import json
import uuid
from datetime import datetime
from contextlib import contextmanager

import mock
from pyramid import testing

from kinto.core import initialization
from kinto.core.events import ResourceChanged, ACTIONS
from kinto.tests.core.support import unittest

from kinto_redis.storage import create_from_config


@contextmanager
def broken_redis():
    from redis import StrictRedis
    old = StrictRedis.lpush

    def push(*args, **kwargs):
        raise Exception('boom')

    StrictRedis.lpush = push
    yield
    StrictRedis.lpush = old

UID = str(uuid.uuid4())


class Resource(object):
    record_id = UID
    timestamp = 123456789


class ViewSet(object):
    def get_name(*args, **kw):
        return 'collection'


class Service(object):
    viewset = ViewSet()


class Match(object):
    cornice_services = {'watev': Service()}
    pattern = 'watev'


class Request(object):
    path = '/1/bucket/collection/'
    prefixed_userid = 'tarek'
    matchdict = {'id': UID}
    registry = matched_route = Match()
    current_resource_name = 'bucket'


class ListenerCalledTest(unittest.TestCase):

    def setUp(self):
        self.config = testing.setUp()
        self.config.add_settings({'events_pool_size': 1,
                                  'events_url': 'redis://localhost:6379/0'})
        self._redis = create_from_config(self.config, prefix='events_')
        self._size = 0

        self.sample_event = ResourceChanged({'action': ACTIONS.CREATE.value},
                                            [],
                                            Request())

    def _save_redis(self):
        self._size = self._redis.llen('kinto.core.events')

    def has_redis_changed(self):
        return self._redis.llen('kinto.core.events') > self._size

    def notify(self, event):
        self._save_redis()
        self.config.registry.notify(event)

    @contextmanager
    def redis_listening(self):
        config = self.config
        listener = 'kinto_redis.listeners'

        # setting up the redis listener
        with mock.patch.dict(config.registry.settings,
                             [('event_listeners', listener),
                              ('event_listeners.redis.pool_size', '1')]):
            initialization.setup_listeners(config)
            config.commit()
            yield

    def test_redis_is_notified(self):
        with self.redis_listening():
            # let's trigger an event
            self.notify(self.sample_event)
            self.assertTrue(self.has_redis_changed())

        # okay, we should have the first event in Redis
        last = self._redis.lpop('kinto.core.events')
        last = json.loads(last.decode('utf8'))
        self.assertEqual(last['action'], ACTIONS.CREATE.value)

    def test_notification_is_broken(self):
        with self.redis_listening():
            # an event with a bad JSON should silently break and send nothing
            # date time objects cannot be dumped
            event2 = ResourceChanged({'action': ACTIONS.CREATE.value,
                                      'somedate': datetime.now()},
                                     [],
                                     Request())
            self.notify(event2)
            self.assertFalse(self.has_redis_changed())

    def test_redis_is_broken(self):
        with self.redis_listening():
            # if the redis call fails, same deal: we should ignore it
            self._save_redis()

            with broken_redis():
                self.config.registry.notify(self.sample_event)

            self.assertFalse(self.has_redis_changed())
