from functools import wraps

import ujson
import redis
from urllib.parse import urlparse

from kinto.core import utils
from kinto.core.decorators import deprecate_kwargs
from kinto.core.storage import (
    exceptions,
    logger,
    DEFAULT_ID_FIELD,
    DEFAULT_MODIFIED_FIELD,
    DEFAULT_DELETED_FIELD,
)
from kinto.core.storage.memory import MemoryBasedStorage


def wrap_redis_error(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except redis.exceptions.RedisError as e:
            logger.exception(e)
            raise exceptions.BackendError(original=e)

    return wrapped


def create_from_config(config, prefix=""):
    """Redis client instantiation from settings.
    """
    settings = config.get_settings()
    uri = settings[prefix + "url"]
    uri = urlparse(uri)
    kwargs = {
        "host": uri.hostname or "localhost",
        "port": uri.port or 6379,
        "password": uri.password or None,
        "db": int(uri.path[1:]) if uri.path else 0,
    }

    pool_size = settings.get(prefix + "pool_size")
    if pool_size is not None:
        kwargs["max_connections"] = int(pool_size)

    block_timeout = settings.get(prefix + "pool_timeout")
    if block_timeout is not None:
        kwargs["timeout"] = float(block_timeout)

    connection_pool = redis.BlockingConnectionPool(**kwargs)
    return redis.StrictRedis(connection_pool=connection_pool)


class Storage(MemoryBasedStorage):
    """Storage backend implementation using Redis.

    .. warning::

        Useful for very low server load, but won't scale since objects sorting
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

    def _encode(self, obj):
        return utils.json.dumps(obj)

    def _decode(self, obj):
        return utils.json.loads(obj.decode("utf-8"))

    @wrap_redis_error
    def flush(self, auth=None):
        self._client.flushdb()

    @wrap_redis_error
    def resource_timestamp(self, resource_name, parent_id, auth=None):
        timestamp = self._client.get(
            "{0}.{1}.timestamp".format(resource_name, parent_id)
        )
        if timestamp:
            return int(timestamp)
        if self.readonly:
            error_msg = (
                "Cannot initialize empty resource timestamp when running in readonly."
            )
            raise exceptions.BackendError(message=error_msg)
        return self.bump_and_store_timestamp(resource_name, parent_id)

    @wrap_redis_error
    def bump_and_store_timestamp(
        self,
        resource_name,
        parent_id,
        obj=None,
        modified_field=None,
        last_modified=None,
    ):

        key = "{0}.{1}.timestamp".format(resource_name, parent_id)
        while 1:
            with self._client.pipeline() as pipe:
                try:
                    pipe.watch(key)
                    current_resource_timestamp = int(pipe.get(key) or 0)

                    current, resource_timestamp = self.bump_timestamp(
                        current_resource_timestamp, obj, modified_field, last_modified
                    )

                    pipe.multi()
                    pipe.set(key, resource_timestamp)
                    pipe.execute()
                    return current
                except redis.WatchError:  # pragma: no cover
                    # Our timestamp has been modified by someone else, let's
                    # retry.
                    # XXX: untested.
                    continue

    @deprecate_kwargs({"collection_id": "resource_name", "record": "obj"})
    @wrap_redis_error
    def create(
        self,
        resource_name,
        parent_id,
        obj,
        id_generator=None,
        id_field=DEFAULT_ID_FIELD,
        modified_field=DEFAULT_MODIFIED_FIELD,
        auth=None,
        ignore_conflict=False,
    ):
        id_generator = id_generator or self.id_generator
        obj = ujson.loads(self.json.dumps(obj))
        if id_field in obj:
            # Raise unicity error if obj with same id already exists.
            try:
                existing = self.get(resource_name, parent_id, obj[id_field])
                raise exceptions.UnicityError(id_field, existing)
            except exceptions.ObjectNotFoundError:
                pass

        _id = obj.setdefault(id_field, id_generator())
        self.set_object_timestamp(
            resource_name, parent_id, obj, modified_field=modified_field
        )

        obj_key = "{0}.{1}.{2}.records".format(resource_name, parent_id, _id)
        with self._client.pipeline() as multi:
            multi.set(obj_key, self._encode(obj))
            multi.sadd("{0}.{1}.records".format(resource_name, parent_id), _id)
            multi.srem("{0}.{1}.deleted".format(resource_name, parent_id), _id)
            multi.execute()

        return obj

    @deprecate_kwargs({"collection_id": "resource_name"})
    @wrap_redis_error
    def get(
        self,
        resource_name,
        parent_id,
        object_id,
        id_field=DEFAULT_ID_FIELD,
        modified_field=DEFAULT_MODIFIED_FIELD,
        auth=None,
    ):
        obj_key = "{0}.{1}.{2}.records".format(resource_name, parent_id, object_id)
        encoded_item = self._client.get(obj_key)
        if encoded_item is None:
            raise exceptions.ObjectNotFoundError(object_id)

        return self._decode(encoded_item)

    @deprecate_kwargs({"collection_id": "resource_name", "record": "obj"})
    @wrap_redis_error
    def update(
        self,
        resource_name,
        parent_id,
        object_id,
        obj,
        id_field=DEFAULT_ID_FIELD,
        modified_field=DEFAULT_MODIFIED_FIELD,
        auth=None,
    ):
        obj = ujson.loads(self.json.dumps(obj))
        obj[id_field] = object_id
        self.set_object_timestamp(
            resource_name, parent_id, obj, modified_field=modified_field
        )

        obj_key = "{0}.{1}.{2}.records".format(resource_name, parent_id, object_id)
        with self._client.pipeline() as multi:
            multi.set(obj_key, self._encode(obj))
            multi.sadd("{0}.{1}.records".format(resource_name, parent_id), object_id)
            multi.srem("{0}.{1}.deleted".format(resource_name, parent_id), object_id)
            multi.execute()

        return obj

    @deprecate_kwargs({"collection_id": "resource_name"})
    @wrap_redis_error
    def delete(
        self,
        resource_name,
        parent_id,
        object_id,
        id_field=DEFAULT_ID_FIELD,
        with_deleted=True,
        modified_field=DEFAULT_MODIFIED_FIELD,
        deleted_field=DEFAULT_DELETED_FIELD,
        auth=None,
        last_modified=None,
    ):
        obj_key = "{0}.{1}.{2}.records".format(resource_name, parent_id, object_id)
        with self._client.pipeline() as multi:
            multi.get(obj_key)
            multi.delete(obj_key)
            multi.srem("{0}.{1}.records".format(resource_name, parent_id), object_id)
            responses = multi.execute()

        encoded_item = responses[0]
        if encoded_item is None:
            raise exceptions.ObjectNotFoundError(object_id)

        existing = self._decode(encoded_item)

        # Need to delete the last_modified field.
        del existing[modified_field]

        self.set_object_timestamp(
            resource_name,
            parent_id,
            existing,
            modified_field=modified_field,
            last_modified=last_modified,
        )
        existing = self.strip_deleted_object(resource_name, parent_id, existing)

        if with_deleted:
            deleted_obj_key = "{0}.{1}.{2}.deleted".format(
                resource_name, parent_id, object_id
            )
            with self._client.pipeline() as multi:
                multi.set(deleted_obj_key, self._encode(existing))
                multi.sadd(
                    "{0}.{1}.deleted".format(resource_name, parent_id), object_id
                )
                multi.execute()

        return existing

    @deprecate_kwargs({"collection_id": "resource_name"})
    @wrap_redis_error
    def purge_deleted(
        self,
        resource_name,
        parent_id,
        before=None,
        id_field=DEFAULT_ID_FIELD,
        modified_field=DEFAULT_MODIFIED_FIELD,
        auth=None,
    ):

        if resource_name is None:
            resource_name = "*"

        keys_pattern = "{0}.{1}.deleted".format(resource_name, parent_id)

        resources_keys = [
            key.decode("utf-8") for key in self._client.scan_iter(match=keys_pattern)
        ]

        resources_keys = [key for key in resources_keys if len(key.split(".")) == 3]
        with self._client.pipeline() as multi:
            for key in resources_keys:
                multi.smembers(key)
            results = multi.execute()

        number_deleted = 0
        for i, ids in enumerate(results):
            if len(ids) == 0:  # pragma: no cover
                continue

            resource_key = resources_keys[i]
            resource_name, parent_id, _ = resource_key.split(".")
            keys = [
                "{0}.{1}.{2}.deleted".format(
                    resource_name, parent_id, _id.decode("utf-8")
                )
                for _id in ids
            ]

            if len(keys) == 0:  # pragma: no cover
                continue

            encoded_results = self._client.mget(keys)
            deleted = [self._decode(r) for r in encoded_results if r]
            if before is not None:
                to_remove = [d["id"] for d in deleted if d[modified_field] < before]
            else:
                to_remove = [d["id"] for d in deleted]

            if len(to_remove) > 0:
                with self._client.pipeline() as pipe:
                    pipe.delete(
                        *[
                            "{0}.{1}.{2}.deleted".format(resource_name, parent_id, _id)
                            for _id in to_remove
                        ]
                    )
                    pipe.srem(resource_key, *to_remove)
                    pipe.execute()
            number_deleted += len(to_remove)
        return number_deleted

    def _get_objects_by_parent_id(self, parent_id, resource_name, with_meta=False):
        if resource_name is None:
            resource_name = "*"

        keys_pattern = "{0}.{1}.records".format(resource_name, parent_id)

        resources_keys = [
            key.decode("utf-8") for key in self._client.scan_iter(match=keys_pattern)
        ]

        resources_keys = [key for key in resources_keys if len(key.split(".")) == 3]
        with self._client.pipeline() as multi:
            for key in resources_keys:
                multi.smembers(key)
            results = multi.execute()

        objects = []
        for i, ids in enumerate(results):
            resource_name, parent_id, _ = resources_keys[i].split(".")

            if len(ids) == 0:  # pragma: no cover
                continue

            objects_keys = [
                "{0}.{1}.{2}.records".format(
                    resource_name, parent_id, _id.decode("utf-8")
                )
                for _id in ids
            ]
            results = self._client.mget(objects_keys)
            if with_meta:
                resource_objects = [
                    dict(
                        __resource_name__=resource_name,
                        __parent_id__=parent_id,
                        **self._decode(r)
                    )
                    for r in results
                    if r
                ]
            else:
                resource_objects = [self._decode(r) for r in results if r]
            objects.extend(resource_objects)

        return objects

    @wrap_redis_error
    def list_all(
        self,
        resource_name,
        parent_id,
        filters=None,
        sorting=None,
        pagination_rules=None,
        limit=None,
        include_deleted=False,
        id_field=DEFAULT_ID_FIELD,
        modified_field=DEFAULT_MODIFIED_FIELD,
        deleted_field=DEFAULT_DELETED_FIELD,
        auth=None,
    ):

        objects = self._get_objects_by_parent_id(parent_id, resource_name)

        deleted = []
        if include_deleted:
            keys_pattern = "{0}.{1}.deleted".format(resource_name, parent_id)

            resources_keys = [
                key.decode("utf-8")
                for key in self._client.scan_iter(match=keys_pattern)
            ]

            resources_keys = [key for key in resources_keys if len(key.split(".")) == 3]
            with self._client.pipeline() as multi:
                for key in resources_keys:
                    multi.smembers(key)
                results = multi.execute()

            deleted = []
            for i, ids in enumerate(results):
                resource_name, parent_id, _ = resources_keys[i].split(".")

                if len(ids) == 0:  # pragma: no cover
                    continue

                deleted_keys = [
                    "{0}.{1}.{2}.deleted".format(
                        resource_name, parent_id, _id.decode("utf-8")
                    )
                    for _id in ids
                ]
                results = self._client.mget(deleted_keys)
                resource_objects = [self._decode(r) for r in results if r]
                deleted.extend(resource_objects)

        objects, _ = self.extract_object_set(
            objects + deleted,
            filters,
            sorting,
            id_field,
            deleted_field,
            pagination_rules,
            limit,
        )

        return objects

    @wrap_redis_error
    def count_all(
        self,
        resource_name,
        parent_id,
        filters=None,
        sorting=None,
        pagination_rules=None,
        limit=None,
        include_deleted=False,
        id_field=DEFAULT_ID_FIELD,
        modified_field=DEFAULT_MODIFIED_FIELD,
        deleted_field=DEFAULT_DELETED_FIELD,
        auth=None,
    ):

        objects = self._get_objects_by_parent_id(parent_id, resource_name)

        _, count = self.extract_object_set(
            objects, filters, sorting, id_field, deleted_field, pagination_rules, limit
        )

        return count

    @deprecate_kwargs({"collection_id": "resource_name"})
    @wrap_redis_error
    def delete_all(
        self,
        resource_name,
        parent_id,
        filters=None,
        sorting=None,
        pagination_rules=None,
        limit=None,
        id_field=DEFAULT_ID_FIELD,
        with_deleted=True,
        modified_field=DEFAULT_MODIFIED_FIELD,
        deleted_field=DEFAULT_DELETED_FIELD,
        auth=None,
    ):
        objects = self._get_objects_by_parent_id(
            parent_id, resource_name, with_meta=True
        )

        objects, count = self.extract_object_set(
            objects=objects,
            filters=filters,
            sorting=sorting,
            pagination_rules=pagination_rules,
            limit=limit,
            id_field=id_field,
            deleted_field=deleted_field,
        )

        deleted = [
            self.delete(
                r.pop("__resource_name__"),
                r.pop("__parent_id__"),
                r[id_field],
                id_field=id_field,
                with_deleted=with_deleted,
                modified_field=modified_field,
                deleted_field=deleted_field,
            )
            for r in objects
        ]
        return deleted


def load_from_config(config):
    client = create_from_config(config, prefix="storage_")
    return Storage(client)
