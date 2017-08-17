from __future__ import absolute_import

import re

from kinto.core.permission import PermissionBase
from kinto_redis.storage import create_from_config, wrap_redis_error


class Permission(PermissionBase):
    """Permission backend implementation using Redis.

    Enable in configuration::

        kinto.permission_backend = kinto_redis.permission

    *(Optional)* Instance location URI can be customized::

        kinto.permission_url = redis://localhost:6379/2

    A threaded connection pool is enabled by default::

        kinto.permission_pool_size = 50

    :noindex:
    """

    def __init__(self, client, *args, **kwargs):
        super(Permission, self).__init__(*args, **kwargs)
        self._client = client

    @property
    def settings(self):
        return dict(self._client.connection_pool.connection_kwargs)

    def initialize_schema(self, dry_run=False):
        # Nothing to do.
        pass

    def _decode_set(self, results):
        return set([r.decode('utf-8') for r in results])

    @wrap_redis_error
    def flush(self):
        self._client.flushdb()

    @wrap_redis_error
    def add_user_principal(self, user_id, principal):
        user_key = 'user:%s' % user_id
        self._client.sadd(user_key, principal)

    @wrap_redis_error
    def remove_user_principal(self, user_id, principal):
        user_key = 'user:%s' % user_id
        self._client.srem(user_key, principal)
        if self._client.scard(user_key) == 0:
            self._client.delete(user_key)

    def remove_principal(self, principal):
        with self._client.pipeline() as pipe:
            user_keys = self._client.scan_iter(match='user:*')
            for user_key in user_keys:
                pipe.srem(user_key, principal)
            pipe.execute()

    @wrap_redis_error
    def get_user_principals(self, user_id):
        # Fetch the groups the user is in.
        user_key = 'user:%s' % user_id
        members = self._decode_set(self._client.smembers(user_key))
        # Fetch the groups system.Authenticated is in.
        group_authenticated = self._decode_set(self._client.smembers('user:system.Authenticated'))
        return members | group_authenticated

    @wrap_redis_error
    def add_principal_to_ace(self, object_id, permission, principal):
        permission_key = 'permission:%s:%s' % (object_id, permission)
        self._client.sadd(permission_key, principal)

    @wrap_redis_error
    def remove_principal_from_ace(self, object_id, permission, principal):
        permission_key = 'permission:%s:%s' % (object_id, permission)
        self._client.srem(permission_key, principal)
        if self._client.scard(permission_key) == 0:
            self._client.delete(permission_key)

    @wrap_redis_error
    def get_object_permission_principals(self, object_id, permission):
        permission_key = 'permission:%s:%s' % (object_id, permission)
        members = self._client.smembers(permission_key)
        return self._decode_set(members)

    @wrap_redis_error
    def get_accessible_objects(self, principals, bound_permissions=None, with_children=True):
        principals = set(principals)

        if bound_permissions:
            keys = ['permission:%s:%s' % op for op in bound_permissions]
            regexp_bound_permissions = [re.compile(k.replace('*', '[^/]+')) for k in keys]
        else:
            keys = ['permission:*']
            regexp_bound_permissions = []

        perms_by_id = dict()
        for key_pattern in keys:
            # By default Redis will include sub-objects.
            # (eg. /buckets/* -> /buckets/<>/collections/<>)
            matching_keys = self._client.scan_iter(match=key_pattern)
            # If no children should be returned, then limit matching keys
            # to those matching the provided bound permissions.
            if not with_children:
                matching_keys = filter_by_regexp(matching_keys, regexp_bound_permissions)

            for key in matching_keys:
                authorized = self._decode_set(self._client.smembers(key))
                if len(authorized & principals) > 0:
                    _, obj_id, permission = key.decode('utf-8').split(':', 2)
                    perms_by_id.setdefault(obj_id, set()).add(permission)

        return perms_by_id

    @wrap_redis_error
    def get_authorized_principals(self, bound_permissions):
        keys = ['permission:%s:%s' % (o, p) for (o, p) in bound_permissions]
        if keys:
            return self._decode_set(self._client.sunion(*list(keys)))
        return set()

    @wrap_redis_error
    def get_objects_permissions(self, objects_ids, permissions=None):
        objects_perms = []
        for object_id in objects_ids:
            if permissions:
                keys = ['permission:%s:%s' % (object_id, permission)
                        for permission in permissions]
            else:
                keys = [key.decode('utf-8') for key in self._client.scan_iter(
                    match='permission:%s:*' % object_id)]

            with self._client.pipeline() as pipe:
                for permission_key in keys:
                    pipe.smembers(permission_key)

                results = pipe.execute()

            permissions = {}
            for i, result in enumerate(results):
                permission = keys[i].split(':', 2)[-1]
                principals = self._decode_set(result)
                if principals:
                    permissions[permission] = principals
            objects_perms.append(permissions)
        return objects_perms

    @wrap_redis_error
    def replace_object_permissions(self, object_id, permissions):
        keys = ['permission:%s:%s' % (object_id, permission)
                for permission in permissions]
        with self._client.pipeline() as pipe:
            for key in keys:
                pipe.delete(key)
                permission = key.split(':', 2)[-1]
                principals = permissions[permission]
                if len(principals) > 0:
                    pipe.sadd(key, *principals)
            pipe.execute()

    @wrap_redis_error
    def delete_object_permissions(self, *object_id_list):
        with self._client.pipeline() as pipe:
            for object_id in object_id_list:
                keys = list(self._client.scan_iter(
                    match='permission:%s:*' % object_id))
                if len(keys) > 0:
                    pipe.delete(*keys)
            pipe.execute()


def filter_by_regexp(keys, regexps):
    results = set()
    for key in keys:
        decoded_key = key.decode('utf-8')
        if not regexps or any([r.match(decoded_key) for r in regexps]):
            results.add(key)
    return list(results)


def load_from_config(config):
    client = create_from_config(config, prefix='permission_')
    return Permission(client)
