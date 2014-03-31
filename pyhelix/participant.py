import kazoo.client
import logging
import os

import accessor
import constants
import helixexec
import znode


class Participant(object):
    """
    Participant Helix connection

    This class encompasses all of a Helix participant's interactions with
    ZooKeeper.
    """
    def __init__(self, cluster_id, host, port, zk_addrs, participant_id=None):
        """
        Initialize the connection parameters.

        Args:
            cluster_id: The cluster this participant should belong to
            host: Host of this participant
            port: Logical port of this participant
            zk_addrs: Comma separated host:port of ZooKeeper servers
            participant_id: (Optional) Custom ID, "host_port" by default
        """
        self._host = host
        self._port = port
        if participant_id:
            self._participant_id = participant_id
        else:
            self._participant_id = '{0}_{1}'.format(host, port)
        self._client = kazoo.client.KazooClient(zk_addrs)
        self._client.add_listener(self._connection_listener)
        self._accessor = accessor.DataAccessor(cluster_id, self._client)
        self._builder = self._accessor.get_key_builder()
        self._callbacks = set()
        self._state_model_ftys = {}
        self._executor = helixexec.HelixExecutor(self._state_model_ftys, self)
        self._pre_connect_callbacks = set()
        self._is_lost = False

    def connect(self):
        """
        Establish a connection.
        """
        # Connect to ZK
        self._is_lost = False
        self._client.start()
        self._init()

    def disconnect(self):
        """
        End an active connection.
        """
        self._is_lost = True
        self._accessor.remove(
            self._builder.live_instance(self._participant_id))
        self._client.stop()
        self._client.close()

    def is_connected(self):
        """
        Get the current connection status.

        Returns:
            True if connected, False if disconnected
        """
        return self._client.connected

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

    def register_pre_connect_callback(self, callback):
        """
        Add a callback that will be invoked just prior to joining.

        Args:
            callback: A function that takes no arguments
        """
        self._pre_connect_callbacks.add(callback)

    def register_state_model_fty(self, state_model_name, state_model_fty):
        """
        Register a state model factory with transition callbacks

        Args:
            state_model_name: The name of the state model definition
            state_model_fty: A factory of callbacks for state transitions
        """
        self._state_model_ftys[state_model_name] = state_model_fty

    def unregister_state_model_fty(self, state_model_name):
        """
        Unregister a state model factory.

        Args:
            state_model_name: The state model for which to ignore transitions
        """
        self._state_model_ftys.pop(state_model_name)

    def _register_message_callback(self, callback):
        """
        Register a callback for messages to this participant (private)

        Args:
            callback: A function that takes a list of message IDs.
        """
        self._callbacks.add(callback)

    def _ensure_participant_config(self):
        """
        Ensure that ZNodes for a participant all exist, non-atomic (private)

        Returns:
            True if everything was persisted, False otherwise
        """
        exists = self._accessor.exists(
            self._builder.participant_config(self._participant_id))
        if not exists and self._auto_join_allowed():
            node = znode.get_empty_znode(self._participant_id)
            node['simpleFields'] = {
                'HELIX_HOST': self._host, 'HELIX_PORT': str(self._port),
                'HELIX_ENABLED': 'true'}
            self._accessor.create(
                self._builder.participant_config(self._participant_id), node)
            self._accessor.create(
                self._builder.instance(self._participant_id), b'')
            self._accessor.create(
                self._builder.current_states(self._participant_id), b'')
            self._accessor.create(
                self._builder.errors(self._participant_id), b'')
            self._accessor.create(
                self._builder.health_report(self._participant_id), b'')
            self._accessor.create(
                self._builder.messages(self._participant_id), b'')
            self._accessor.create(
                self._builder.status_updates(self._participant_id), b'')
        elif not exists:
            return False
        return True

    def _auto_join_allowed(self):
        """
        Check if this cluster supports participants auto-joining (private)

        Returns:
            True if auto join allowed, False otherwise
        """
        clst_config = self._accessor.get(self._builder.cluster_config())
        if clst_config:
            if 'allowParticipantAutoJoin' in clst_config['simpleFields']:
                auto_join = (
                    clst_config['simpleFields']['allowParticipantAutoJoin'])
                if auto_join == 'true':
                    return True
        return False

    def _create_live_instance_node(self):
        """
        Create a live instance ZNode for this participant (private)

        Returns:
            True if created, False otherwise
        """
        node = znode.get_empty_znode(self._participant_id)
        node['simpleFields'] = {
            'HELIX_VERSION': 'pyhelix-{0}'.format(constants.CURRENT_VERSION),
            'SESSION_ID': str(self._client.client_id[0]),
            'LIVE_INSTANCE': '{0}@{1}'.format(os.getpid(), self._host)}
        return self._accessor.create(
            self._builder.live_instance(self._participant_id), node)

    def _message_handler(self, messages):
        """
        Dispatcher for message callbacks (private)

        Args:
            List of message ids as strings

        Returns:
            Always True
        """
        logging.info('handler called: {0}'.format(messages))
        for cb in self._callbacks:
            message_nodes = []
            for message_id in messages:
                message_nodes.append(self._accessor.get(
                    self._builder.message(self._participant_id, message_id)))
            cb(message_nodes)
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
        # Register with the cluster
        result = self._ensure_participant_config()
        if not result:
            logging.error('Participant configuration could not be added')
            self.disconnect()
            return

        # Get ready to receive cluster messages
        self._register_message_callback(self._executor.on_message)
        self._accessor.watch_children(self._builder.messages(
            self._participant_id), self._message_handler)

        # Invoke pre-connect callbacks
        for pre_connect_callback in self._pre_connect_callbacks:
            pre_connect_callback()

        # Create an ephemeral node
        result = self._create_live_instance_node()
        if not result:
            logging.error('Could not create live instance')
            self.disconnect()
            return
