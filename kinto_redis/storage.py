from __future__ import absolute_import, unicode_literals
from functools import wraps

import ujson
import redis
from six.moves.urllib import parse as urlparse

from kinto.core import utils
from kinto.core.storage import (
    exceptions, logger, DEFAULT_ID_FIELD,
    DEFAULT_MODIFIED_FIELD, DEFAULT_DELETED_FIELD)
from kinto.core.storage.memory import MemoryBasedStorage


def wrap_redis_error(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except redis.RedisError as e:
            logger.exception(e)
            raise exceptions.BackendError(original=e)
    return wrapped


def create_from_config(config, prefix=''):
    """Redis client instantiation from settings.
    """
    settings = config.get_settings()
    uri = settings[prefix + 'url']
    uri = urlparse.urlparse(uri)
    pool_size = int(settings[prefix + 'pool_size'])
    kwargs = {
        "max_connections": pool_size,
        "host": uri.hostname or 'localhost',
        "port": uri.port or 6379,
        "password": uri.password or None,
        "db": int(uri.path[1:]) if uri.path else 0
    }
    block_timeout = settings.get(prefix + 'pool_timeout')
    if block_timeout is not None:
        kwargs["timeout"] = float(block_timeout)

    connection_pool = redis.BlockingConnectionPool(**kwargs)
    return redis.StrictRedis(connection_pool=connection_pool)


class Storage(MemoryBasedStorage):
    """Storage backend implementation using Redis.

    .. warning::

        Useful for very low server load, but won't scale since records sorting
        and filtering are performed in memory.

    Enable in configuration::

        kinto.storage_backend = kinto_redis.storage

    *(Optional)* Instance location URI can be customized::

        kinto.storage_url = redis://localhost:6379/0

    A threaded connection pool is enabled by default::

        kinto.storage_pool_size = 50
    """

    def __init__(self, client, *args, readonly=False, **kwargs):
        super(Storage, self).__init__(*args, **kwargs)
        self.readonly = readonly
        self._client = client

    @property
    def settings(self):
        return dict(self._client.connection_pool.connection_kwargs)

    def _encode(self, record):
        return utils.json.dumps(record)

    def _decode(self, record):
        return utils.json.loads(record.decode('utf-8'))

    @wrap_redis_error
    def flush(self, auth=None):
        self._client.flushdb()

    @wrap_redis_error
    def collection_timestamp(self, collection_id, parent_id, auth=None):
        timestamp = self._client.get(
            '{0}.{1}.timestamp'.format(collection_id, parent_id))
        if timestamp:
            return int(timestamp)
        if self.readonly:
            error_msg = 'Cannot initialize empty collection timestamp when running in readonly.'
            raise exceptions.BackendError(message=error_msg)
        return self.bump_and_store_timestamp(collection_id, parent_id)

    @wrap_redis_error
    def bump_and_store_timestamp(self, collection_id, parent_id, record=None,
                                 modified_field=None, last_modified=None):

        key = '{0}.{1}.timestamp'.format(collection_id, parent_id)
        while 1:
            with self._client.pipeline() as pipe:
                try:
                    pipe.watch(key)
                    current_collection_timestamp = int(pipe.get(key) or 0)

                    current, collection_timestamp = self.bump_timestamp(
                        current_collection_timestamp,
                        record, modified_field,
                        last_modified)

                    pipe.multi()
                    pipe.set(key, collection_timestamp)
                    pipe.execute()
                    return current
                except redis.WatchError:  # pragma: no cover
                    # Our timestamp has been modified by someone else, let's
                    # retry.
                    # XXX: untested.
                    continue

    @wrap_redis_error
    def create(self, collection_id, parent_id, record, id_generator=None,
               id_field=DEFAULT_ID_FIELD,
               modified_field=DEFAULT_MODIFIED_FIELD,
               auth=None, ignore_conflict=False):
        id_generator = id_generator or self.id_generator
        record = ujson.loads(self.json.dumps(record))
        if id_field in record:
            # Raise unicity error if record with same id already exists.
            try:
                existing = self.get(collection_id, parent_id, record[id_field])
                raise exceptions.UnicityError(id_field, existing)
            except exceptions.RecordNotFoundError:
                pass

        _id = record.setdefault(id_field, id_generator())
        self.set_record_timestamp(collection_id, parent_id, record,
                                  modified_field=modified_field)

        record_key = '{0}.{1}.{2}.records'.format(collection_id,
                                                  parent_id,
                                                  _id)
        with self._client.pipeline() as multi:
            multi.set(
                record_key,
                self._encode(record)
            )
            multi.sadd(
                '{0}.{1}.records'.format(collection_id, parent_id),
                _id
            )
            multi.srem(
                '{0}.{1}.deleted'.format(collection_id, parent_id),
                _id
            )
            multi.execute()

        return record

    @wrap_redis_error
    def get(self, collection_id, parent_id, object_id,
            id_field=DEFAULT_ID_FIELD,
            modified_field=DEFAULT_MODIFIED_FIELD,
            auth=None):
        record_key = '{0}.{1}.{2}.records'.format(collection_id,
                                                  parent_id,
                                                  object_id)
        encoded_item = self._client.get(record_key)
        if encoded_item is None:
            raise exceptions.RecordNotFoundError(object_id)

        return self._decode(encoded_item)

    @wrap_redis_error
    def update(self, collection_id, parent_id, object_id, record,
               id_field=DEFAULT_ID_FIELD,
               modified_field=DEFAULT_MODIFIED_FIELD,
               auth=None):
        record = ujson.loads(self.json.dumps(record))
        record[id_field] = object_id
        self.set_record_timestamp(collection_id, parent_id, record,
                                  modified_field=modified_field)

        record_key = '{0}.{1}.{2}.records'.format(collection_id,
                                                  parent_id,
                                                  object_id)
        with self._client.pipeline() as multi:
            multi.set(
                record_key,
                self._encode(record)
            )
            multi.sadd(
                '{0}.{1}.records'.format(collection_id, parent_id),
                object_id
            )
            multi.srem(
                '{0}.{1}.deleted'.format(collection_id, parent_id),
                object_id
            )
            multi.execute()

        return record

    @wrap_redis_error
    def delete(self, collection_id, parent_id, object_id,
               id_field=DEFAULT_ID_FIELD, with_deleted=True,
               modified_field=DEFAULT_MODIFIED_FIELD,
               deleted_field=DEFAULT_DELETED_FIELD,
               auth=None, last_modified=None):
        record_key = '{0}.{1}.{2}.records'.format(collection_id,
                                                  parent_id,
                                                  object_id)
        with self._client.pipeline() as multi:
            multi.get(record_key)
            multi.delete(record_key)
            multi.srem(
                '{0}.{1}.records'.format(collection_id, parent_id),
                object_id
            )
            responses = multi.execute()

        encoded_item = responses[0]
        if encoded_item is None:
            raise exceptions.RecordNotFoundError(object_id)

        existing = self._decode(encoded_item)

        # Need to delete the last_modified field.
        del existing[modified_field]

        self.set_record_timestamp(collection_id, parent_id, existing,
                                  modified_field=modified_field,
                                  last_modified=last_modified)
        existing = self.strip_deleted_record(collection_id, parent_id,
                                             existing)

        if with_deleted:
            deleted_record_key = '{0}.{1}.{2}.deleted'.format(collection_id,
                                                              parent_id,
                                                              object_id)
            with self._client.pipeline() as multi:
                multi.set(
                    deleted_record_key,
                    self._encode(existing)
                )
                multi.sadd(
                    '{0}.{1}.deleted'.format(collection_id, parent_id),
                    object_id
                )
                multi.execute()

        return existing

    @wrap_redis_error
    def purge_deleted(self, collection_id, parent_id, before=None,
                      id_field=DEFAULT_ID_FIELD,
                      modified_field=DEFAULT_MODIFIED_FIELD,
                      auth=None):

        if collection_id is None:
            collection_id = '*'

        keys_pattern = '{0}.{1}.deleted'.format(collection_id, parent_id)

        collections_keys = [key.decode('utf-8') for key in
                            self._client.scan_iter(match=keys_pattern)]

        collections_keys = [key for key in collections_keys
                            if len(key.split('.')) == 3]
        with self._client.pipeline() as multi:
            for key in collections_keys:
                multi.smembers(key)
            results = multi.execute()

        number_deleted = 0
        for i, ids in enumerate(results):
            if len(ids) == 0:  # pragma: no cover
                continue

            collection_key = collections_keys[i]
            collection_id, parent_id, _ = collection_key.split('.')
            keys = ['{0}.{1}.{2}.deleted'.format(collection_id, parent_id,
                                                 _id.decode('utf-8'))
                    for _id in ids]

            if len(keys) == 0:  # pragma: no cover
                continue

            encoded_results = self._client.mget(keys)
            deleted = [self._decode(r) for r in encoded_results if r]
            if before is not None:
                to_remove = [d['id'] for d in deleted
                             if d[modified_field] < before]
            else:
                to_remove = [d['id'] for d in deleted]

            if len(to_remove) > 0:
                with self._client.pipeline() as pipe:
                    pipe.delete(*['{0}.{1}.{2}.deleted'.format(
                        collection_id, parent_id, _id) for _id in to_remove])
                    pipe.srem(collection_key, *to_remove)
                    pipe.execute()
            number_deleted += len(to_remove)
        return number_deleted

    def _get_objects_by_parent_id(self, parent_id, collection_id, with_meta=False):
        if collection_id is None:
            collection_id = '*'

        keys_pattern = '{0}.{1}.records'.format(collection_id, parent_id)

        collections_keys = [key.decode('utf-8') for key in
                            self._client.scan_iter(match=keys_pattern)]

        collections_keys = [key for key in collections_keys
                            if len(key.split('.')) == 3]
        with self._client.pipeline() as multi:
            for key in collections_keys:
                multi.smembers(key)
            results = multi.execute()

        records = []
        for i, ids in enumerate(results):
            collection_id, parent_id, _ = collections_keys[i].split('.')

            if len(ids) == 0:  # pragma: no cover
                continue

            records_keys = ['{0}.{1}.{2}.records'.format(collection_id,
                                                         parent_id,
                                                         _id.decode('utf-8'))
                            for _id in ids]
            results = self._client.mget(records_keys)
            if with_meta:
                collection_records = [dict(__collection_id__=collection_id,
                                           __parent_id__=parent_id,
                                           **self._decode(r))
                                      for r in results if r]
            else:
                collection_records = [self._decode(r) for r in results if r]
            records.extend(collection_records)

        return records

    @wrap_redis_error
    def get_all(self, collection_id, parent_id, filters=None, sorting=None,
                pagination_rules=None, limit=None, include_deleted=False,
                id_field=DEFAULT_ID_FIELD,
                modified_field=DEFAULT_MODIFIED_FIELD,
                deleted_field=DEFAULT_DELETED_FIELD,
                auth=None):

        records = self._get_objects_by_parent_id(parent_id, collection_id)

        deleted = []
        if include_deleted:
            keys_pattern = '{0}.{1}.deleted'.format(collection_id, parent_id)

            collections_keys = [key.decode('utf-8') for key in
                                self._client.scan_iter(match=keys_pattern)]

            collections_keys = [key for key in collections_keys
                                if len(key.split('.')) == 3]
            with self._client.pipeline() as multi:
                for key in collections_keys:
                    multi.smembers(key)
                results = multi.execute()

            deleted = []
            for i, ids in enumerate(results):
                collection_id, parent_id, _ = collections_keys[i].split('.')

                if len(ids) == 0:  # pragma: no cover
                    continue

                deleted_keys = ['{0}.{1}.{2}.deleted'.format(collection_id,
                                                             parent_id,
                                                             _id.decode('utf-8'))
                                for _id in ids]
                results = self._client.mget(deleted_keys)
                collection_records = [self._decode(r) for r in results if r]
                deleted.extend(collection_records)

        records, count = self.extract_record_set(records + deleted,
                                                 filters, sorting,
                                                 id_field, deleted_field,
                                                 pagination_rules, limit)

        return records, count

    @wrap_redis_error
    def delete_all(self, collection_id, parent_id, filters=None,
                   sorting=None, pagination_rules=None, limit=None,
                   id_field=DEFAULT_ID_FIELD, with_deleted=True,
                   modified_field=DEFAULT_MODIFIED_FIELD,
                   deleted_field=DEFAULT_DELETED_FIELD,
                   auth=None):
        records = self._get_objects_by_parent_id(parent_id, collection_id, with_meta=True)

        records, count = self.extract_record_set(records=records,
                                                 filters=filters,
                                                 sorting=sorting,
                                                 pagination_rules=pagination_rules, limit=limit,
                                                 id_field=id_field,
                                                 deleted_field=deleted_field)

        deleted = [self.delete(r.pop('__collection_id__'),
                               r.pop('__parent_id__'),
                               r[id_field],
                               id_field=id_field, with_deleted=with_deleted,
                               modified_field=modified_field,
                               deleted_field=deleted_field)
                   for r in records]
        return deleted


def load_from_config(config):
    client = create_from_config(config, prefix='storage_')
    return Storage(client)
