import json
import kazoo.exceptions
import kazoo.recipe.watchers
import logging
import os
import traceback

import keybuilder


class DataAccessor(object):
    """
    Helix property accessor
    """
    def __init__(self, cluster_id, zk_client):
        """
        Initialze for a cluster and ZooKeeper connection

        Args:
            cluster_id: Unique clsuter identifier
            zk_client: A live connection to ZooKeeper
        """
        self._cluster_id = cluster_id
        self._client = zk_client

    def create(self, key, data):
        """
        Create a property, creating parent nodes as necessary

        Args:
            key: KeyBuilder property
            data: The data to write (python object)

        Returns:
            True if successful, False otherwise
        """
        path = key['path']
        try:
            node = data
            if data:
                node = json.dumps(data, indent=2, sort_keys=True)
            logging.info('creating {0} with {1}'.format(path, data))
            self._client.create(
                path, node, ephemeral=key['ephemeral'],
                sequence=key['sequential'], makepath=True)
            return True
        except kazoo.exceptions.NodeExistsError:
            logging.warn('{0} exists already'.format(path))
            return self.set(key, data)
        except kazoo.exceptions.KazooException:
            logging.error(path)
            logging.error(traceback.format_exc())
        return False

    def set(self, key, data):
        """
        Set a property, creating parent nodes as necessary

        Args:
            key: KeyBuilder property
            data: The data to write (python object)

        Returns:
            True if successful, False otherwise
        """
        if data:
            data = json.dumps(data, indent=2, sort_keys=True)
        path = key['path']
        try:
            if not key['update_only_on_exists']:
                self._client.ensure_path(path)
            logging.info('setting {0} with {1}'.format(path, data))
            self._client.set(path, data)
            return True
        except kazoo.exceptions.NoNodeError:
            logging.info('{0} does not exist'.format(path))
        except kazoo.exceptions.KazooException:
            logging.error(path)
            logging.error(traceback.format_exc())
        return False

    def get(self, key):
        """
        Get a property

        Args:
            key: KeyBuilder property

        Returns:
            The data that is persisted, or None
        """
        path = key['path']
        try:
            value, stat = self._client.get(path)
            return json.loads(value)
        except kazoo.exceptions.NoNodeError:
            logging.info('{0} does not exist'.format(path))
        except kazoo.exceptions.KazooException:
            logging.error(path)
            logging.error(traceback.format_exc())
        return None

    def get_children(self, key):
        """
        Get the children of a property

        Args:
            key: KeyBuilder property

        Returns:
            List of child names
        """
        path = key['path']
        try:
            return self._client.get_children(path)
        except kazoo.exceptions.NoNodeError:
            logging.info('{0} does not exist'.format(path))
        except kazoo.exceptions.KazooException:
            logging.error(path)
            logging.error(traceback.format_exc())
        return []

    def update(self, key, updated_value, sub=False):
        """
        Update a property

        Args:
            key: KeyBuilder property
            updated_value: The data to write (python object)
            sub: True to subtract the updated value, False (default) to add it

        Returns:
            True if successful, False otherwise
        """
        path = key['path']
        done = False
        while not done:
            try:
                exists_stat = self._client.exists(path)
                if not exists_stat:
                    if key['update_only_on_exists'] or sub:
                        logging.info(
                            '{0} does not exist, cannot update'.format(path))
                        return False
                    try:
                        node = updated_value
                        if updated_value:
                            node = json.dumps(updated_value, indent=2, sort_keys=True)
                        self._client.create(
                            path, node, ephemeral=key['ephemeral'],
                            sequence=key['sequential'], makepath=True)
                        return True
                    except kazoo.exceptions.NodeExistsError:
                        pass  # ignore failure here
                value, get_stat = self._client.get(path)
                value = json.loads(value)
                if key['merge_on_update']:
                    # merge in if allowed
                    if not sub:
                        for k, v in updated_value['simpleFields'].iteritems():
                            value['simpleFields'][k] = v
                        for k, v in updated_value['listFields'].iteritems():
                            value['listFields'][k] = v
                        for k, v in updated_value['mapFields'].iteritems():
                            value['mapFields'][k] = v
                    else:
                        for k, v in updated_value['simpleFields'].iteritems():
                            if k in value['simpleFields']:
                                value['simpleFields'].pop(k)
                        for k, v in updated_value['listFields'].iteritems():
                            if k in value['listFields']:
                                value['listFields'].pop(k)
                        for k, v in updated_value['mapFields'].iteritems():
                            if k in value['mapFields']:
                                value['mapFields'].pop(k)
                elif not sub:
                    # otherwise, replace entirely if not a subtraction
                    value = updated_value
                else:
                    # merge must be allowed to subtract
                    logging.warn(
                        'Tried to do a subtract on a property that'
                        ' doesn\'t allow merge')
                    return False
                value = json.dumps(value, indent=2, sort_keys=True)
                update_stat = self._client.set(
                    path, value, version=get_stat.version)
                if update_stat:
                    done = True
            except kazoo.exceptions.BadVersionError:
                logging.info('trying again to update {0}'.format(path))
                continue  # ignore this, try again
            except kazoo.exceptions.KazooException:
                logging.error(path)
                logging.error(traceback.format_exc())
                return False
        return True

    def remove(self, key):
        """
        Remove a property

        Args:
            key: KeyBuilder property

        Returns:
            True if successful, False otherwise
        """
        path = key['path']
        try:
            self._client.delete(path, recursive=True)
            return True
        except kazoo.exceptions.NoNodeError:
            logging.warn('{0} does not exist'.format(path))
        except kazoo.exceptions.KazooException:
            logging.error(path)
            logging.error(traceback.format_exc())
        return False

    def exists(self, key):
        """
        Check if a property exists

        Args:
            key: KeyBuilder property

        Returns:
            True if exists, False otherwise
        """
        path = key['path']
        try:
            return self._client.exists(path) is not None
        except kazoo.exceptions.KazooException:
            logging.error(path)
            logging.error(traceback.format_exc())
        return False

    def watch_children(self, key, func):
        """
        Watch children of a property

        Args:
            key: KeyBuilder property
            func: Single argument callback
        """
        path = key['path']
        kazoo.recipe.watchers.ChildrenWatch(self._client, path, func=func)

    def watch_property(self, key, func):
        """
        Watch a property

        Args:
            key: KeyBuilder property
            func: Two-argument callback that takes data, stat
        """
        path = key['path']
        kazoo.recipe.watchers.DataWatch(self._client, path, func=func)

    def get_key_builder(self):
        """
        Get a key builder that can be used with this accessor.

        Returns:
            KeyBuilder instance
        """
        return keybuilder.KeyBuilder(self._cluster_id)
