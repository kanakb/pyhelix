import kazoo.client
import logging
import os

import accessor
import helixexec
import znode

class Participant(object):
    """
    Participant Helix connection

    This class encompasses all of a Helix participant's interactions with ZooKeeper.
    """
    def __init__(self, cluster_id, host, port, zk_addrs):
        """
        Initialize the connection parameters.

        Args:
            cluster_id: The cluster this participant should belong to
            host: Host of this participant
            port: Logical port of this participant
            zk_addrs: Comma separated host:port of ZooKeeper servers
        """
        self._host = host
        self._port = port
        self._participant_id = '{0}_{1}'.format(host, port)
        self._client = kazoo.client.KazooClient(zk_addrs)
        self._accessor = accessor.DataAccessor(cluster_id, self._client)
        self._builder = self._accessor.get_key_builder()
        self._callbacks = set()
        self._session_id = None
        self._connected = False
        self._state_model_ftys = {}
        self._executor = helixexec.HelixExecutor(self._state_model_ftys, self)

    def connect(self):
        """
        Establish a connection.
        """
        self._client.start()
        result = self._ensure_participant_config()
        if not result:
            logging.error('Participant configuration could not be added')
            self.disconnect()
            return
        self._register_message_callback(self._executor.on_message)
        self._accessor.watch_children(self._builder.messages(self._participant_id),
            self._message_handler)
        result = self._create_live_instance_node()
        if not result:
            logging.error('Could not create live instance')
            self.disconnect()
            return
        self._connected = True

    def disconnect(self):
        """
        End an active connection.
        """
        self._accessor.remove(self._builder.live_instance(self._participant_id))
        self._callbacks = set()
        self._client.stop()
        self._connected = False

    def is_connected(self):
        """
        Get the current connection status.

        Returns:
            True if connected, False if disconnected
        """
        if not self._client.connected:
            self._connected = False
        return self._connected

    def get_accessor(self):
        """
        Get a DataAccessor for this cluster.

        Returns:
            Instantiated DataAccessor
        """
        return self._accessor

    def get_participant_id(self):
        """
        Get the ID of this participant

        Returns:
            String participant ID
        """
        return self._participant_id

    def get_session_id(self):
        """
        Get the ID of the current ZooKeeper session

        Returns:
            Session ID, or None if not connected
        """
        if not self.is_connected():
            logging.error('Cannot get session ID, not connected')
            return None
        return str(self._client.client_id[0])

    def _register_message_callback(self, callback):
        """
        Register a callback for messages to this participant (private)

        Args:
            callback: A function that takes a list of message IDs as an argument.
        """
        self._callbacks.add(callback)

    def register_state_model_fty(self, state_model_name, state_model_fty):
        """
        Register a state model factory with transition callbacks

        Args:
            state_model_name: The name of the state model definition
            state_model_fty: A factory of callbacks for state transitions
        """
        self._state_model_ftys[state_model_name] = state_model_fty

    def unregister_state_model_fty(self, state_model_name):
        self._state_model_ftys.pop(state_model_name)

    def _ensure_participant_config(self):
        """
        Ensure that the ZNodes for a participant all exist - this is not atomic (private)

        Returns:
            True if everything was persisted, False otherwise
        """
        exists = self._accessor.exists(self._builder.participant_config(self._participant_id))
        if not exists and self._auto_join_allowed():
            node = znode.get_empty_znode(self._participant_id)
            node['simpleFields'] = {'HELIX_HOST': self._host, 'HELIX_PORT': str(self._port),
                'HELIX_ENABLED': 'true'}
            self._accessor.create(self._builder.participant_config(self._participant_id), node)
            self._accessor.create(self._builder.instance(self._participant_id), b'')
            self._accessor.create(self._builder.current_states(self._participant_id), b'')
            self._accessor.create(self._builder.errors(self._participant_id), b'')
            self._accessor.create(self._builder.health_report(self._participant_id), b'')
            self._accessor.create(self._builder.messages(self._participant_id), b'')
            self._accessor.create(self._builder.status_updates(self._participant_id), b'')
        elif not exists:
            return False
        return True

    def _auto_join_allowed(self):
        """
        Check if this cluster supports participants automatically joining (private)

        Returns:
            True if auto join allowed, False otherwise
        """
        clst_config = self._accessor.get(self._builder.cluster_config())
        if clst_config:
            if 'allowParticipantAutoJoin' in clst_config['simpleFields']:
                if clst_config['simpleFields']['allowParticipantAutoJoin'] == 'true':
                    return True
        return False

    def _create_live_instance_node(self):
        """
        Create a live instance ZNode for this participant (private)

        Returns:
            True if created, False otherwise
        """
        node = znode.get_empty_znode(self._participant_id)
        node['simpleFields'] = {'HELIX_VERSION': 'pyhelix-0.1',
            'SESSION_ID': str(self._client.client_id[0]),
            'LIVE_INSTANCE': '{0}@{1}'.format(os.getpid(), self._host)}
        return self._accessor.create(self._builder.live_instance(self._participant_id),
            node)

    def _message_handler(self, messages):
        """
        Dispatcher for message callbacks.

        Args:
            List of message ids as strings

        Returns:
            Always True
        """
        logging.info('handler called: {0}'.format(messages))
        for cb in self._callbacks:
            message_nodes = []
            for message_id in messages:
                message_nodes.append(self._accessor.get(self._builder.message(self._participant_id,
                    message_id)))
            cb(message_nodes)
        return True
