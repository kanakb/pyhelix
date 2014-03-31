import json
import kazoo.client
import logging

import accessor


class SpectatorConnection(object):
    """
    Helix spectator connection

    This class encompasses all of a spectator's interactions with ZooKeeper.
    """
    def __init__(self, cluster_id, zk_addrs):
        """
        Initialize the connection parameters

        Args:
            cluster_id: The cluster to watch
            zk_addrs: Comma-separated host:port of ZooKeeper servers
        """
        self._cluster_id = cluster_id
        self._client = kazoo.client.KazooClient(zk_addrs)
        self._client.add_listener(self._connection_listener)
        self._accessor = accessor.DataAccessor(cluster_id, self._client)
        self._keybuilder = self._accessor.get_key_builder()
        self._spectators = {}
        self._participants = {}
        self._is_lost = False

    def connect(self):
        """
        Establish a connection
        """
        self._is_lost = False
        self._client.start()
        self._init()

    def disconnect(self):
        """
        End an active connection
        """
        self._is_lost = True
        self._client.stop()

    def is_connected(self):
        """
        Get the current connection status

        Returns:
            True if connected, False otherwise
        """
        return self._client.connected

    def spectate(self, resource_id):
        """
        Start spectating on a resource.

        Args:
            resource_id: The resource of interest

        Returns:
            A Spectator object
        """
        if not self.is_connected():
            logging.error(
                'Tried to spectate on {0} without connecting!'.format(
                    resource_id))
            return None
        if resource_id in self._spectators:
            return self._spectators[resource_id]
        logging.debug('About to start watching {0}'.format(resource_id))
        s = Spectator(self._accessor, resource_id, self._participants)
        self._spectators[resource_id] = s
        return s

    def get_accessor(self):
        """
        Get a DataAccessor for this cluster.

        Returns:
            Instantiated DataAccessor
        """
        return self._accessor

    def _pc_parent_watcher(self, children):
        """
        Callback for participant config children (private)

        Args:
            children: List of child names

        Returns:
            Always True
        """
        if not children:
            return True
        for child in children:
            if child not in self._participants:
                # Watch each participant config
                self._accessor.watch_property(
                    self._keybuilder.participant_config(child),
                    self._pc_watcher)
        return True

    def _pc_watcher(self, data, stat):
        """
        Callback for participant config change (private)

        Args:
            data: The ZNode data
            stat: The ZNode stat

        Returns:
            Always True
        """
        if not data:
            return True
        participant_config = json.loads(data)
        if participant_config and 'id' in participant_config:
            self._participants[participant_config['id']] = participant_config
        return True

    def _connection_listener(self, state):
        """
        Callback for connection state changes (private)

        Args:
            state: the current connection state (LOST, CONNECTED, SUSPENDED)
        """
        if state == kazoo.client.KazooState.LOST:
            self._is_lost = True
        elif self._is_lost and state == kazoo.client.KazooState.CONNECTED:
            self._client.handler.spawn(self._init)
            self._is_lost = False

    def _init(self):
        """
        Internal initialization (private)
        """
        self._participants.clear()
        self._accessor.watch_children(
            self._keybuilder.participant_configs(), self._pc_parent_watcher)
        for resource_id, s in self._spectators.iteritems():
            s._init(resource_id)


class Spectator(object):
    """
    Helix spectator
    """
    def __init__(self, accessor, resource_id, participants):
        """
        Initialize a spectator for a resource

        Args:
            accessor: Instantiated DataAccessor
            resource_id: The resource to spectate
            participants: Map of participant id to participant config
        """
        self._mapping = {}
        self._participants = participants
        self._accessor = accessor
        self._keybuilder = accessor.get_key_builder()
        self._init(resource_id)

    def get_participants(self, state, partition_id=None):
        """
        Get all participants in a given state (optionally for a partition)

        Args:
            state: The state
            partition_id: The partition (optional)

        Returns:
            List of participants
        """
        result = set()
        partitions = None
        if partition_id:
            partitions = [partition_id]
        else:
            partitions = self._mapping.keys()
        for partition_id in partitions:
            for participant_id, s in self._mapping[partition_id].iteritems():
                if s == state:
                    result.add(participant_id)
        return [self._participants[p] for p in result]

    def get_state_map(self, partition_id):
        """
        Get a mapping of participant to state for a partition

        Args:
            partition_id: The partition

        Returns:
            Map of participant id to state
        """
        if partition_id in self._mapping:
            return self._mapping[partition_id]
        else:
            return {}

    def _ev_watcher(self, data, stat):
        """
        Callback for external view change (private)

        Args:
            data: The ZNode data
            stat: The ZNode metadata

        Returns:
            Always True
        """
        if not data:
            self._mapping = {}
            return True
        external_view = json.loads(data)
        if (external_view and
           'mapFields' in external_view and external_view['mapFields']):
            self._mapping = external_view['mapFields']
        else:
            self._mapping = {}
        logging.debug('Updated external view: {0}'.format(self._mapping))
        return True

    def _init(self, resource_id):
        """
        Internal initialization (private)

        Args:
            resource_id: The resource to spectate on
        """
        self._accessor.watch_property(
            self._keybuilder.external_view(resource_id), self._ev_watcher)
