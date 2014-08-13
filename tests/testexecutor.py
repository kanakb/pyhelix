import unittest

import pyhelix.accessor as accessor
import pyhelix.helixexec as helixexec
import pyhelix.statemodel as statemodel
import pyhelix.znode as znode

import mockparticipant


class MockStateModel(statemodel.StateModel):
    """
    A state model with nop transitions
    """
    def default_transition_handler(self, message):
        pass


class MockStateModelFactory(statemodel.StateModel):
    """
    A factory for a nop state model
    """
    def create_state_model(self, partition_name):
        """
        Create a MockStateModel

        Args:
            partition_name: The partition

        Returns:
            MockStateModel
        """
        return MockStateModel()


class TestExecutor(unittest.TestCase):
    """
    These test methods ensure proper parsing/handling of messages.
    """
    def setUp(self):
        factories = {'OnlineOffline': MockStateModelFactory()}
        self._p = mockparticipant.MockParticipant(
            'mockcluster', 'localhost', 1234, 'localhost:2181')
        self._p.connect()
        self.executor = helixexec.HelixExecutor(factories, self._p)

    def test_session_id_mismatch(self):
        """
        Test that the executor removes messages with incorrect session ID
        """
        message = znode.get_empty_znode('MY_MESSAGE_ID')
        message['simpleFields']['MSG_TYPE'] = 'STATE_TRANSITION'
        message['simpleFields']['TGT_SESSION_ID'] = 'wrong'
        accessor = self._p.get_accessor()
        keybuilder = accessor.get_key_builder()
        participant_id = self._p.get_participant_id()
        message_id = message['id']
        message_key = keybuilder.message(participant_id, message_id)
        accessor.create(message_key, message)
        self.executor.on_message([message])
        self.assertFalse(accessor.exists(message_key))
