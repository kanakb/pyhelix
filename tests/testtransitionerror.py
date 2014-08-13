import unittest

import pyhelix.accessor as accessor
import pyhelix.helixtask as helixtask
import pyhelix.statemodel as statemodel
import pyhelix.znode as znode

import mockparticipant


class FailingStateModel(statemodel.StateModel):
    """
    A state model with failing transitions
    """
    def default_transition_handler(self, message):
        raise Exception('Failed transition, expected')


class TestTransitionError(unittest.TestCase):
    """
    These test methods ensure proper parsing/handling of messages.
    """
    def setUp(self):
        self._p = mockparticipant.MockParticipant(
            'mockcluster', 'localhost', 1234, 'localhost:2181')
        self._p.connect()

    def test_failed_transition(self):
        """
        Test that the executor removes messages with incorrect session ID
        """
        resource = 'myResource'
        partition = '{0}_0'.format(resource)
        session_id = self._p.get_session_id()
        state_model_def = 'OnlineOffline'
        message = znode.get_empty_znode('MY_MESSAGE_ID')
        message['simpleFields']['MSG_TYPE'] = 'STATE_TRANSITION'
        message['simpleFields']['TGT_SESSION_ID'] = session_id
        message['simpleFields']['FROM_STATE'] = 'OFFLINE'
        message['simpleFields']['TO_STATE'] = 'ONLINE'
        message['simpleFields']['RESOURCE_NAME'] = resource
        message['simpleFields']['PARTITION_NAME'] = partition
        message['simpleFields']['STATE_MODEL_DEF'] = state_model_def
        accessor = self._p.get_accessor()
        keybuilder = accessor.get_key_builder()
        participant_id = self._p.get_participant_id()
        message_id = message['id']
        message_key = keybuilder.message(participant_id, message_id)
        accessor.create(message_key, message)
        msg_task = helixtask.HelixTask(message, FailingStateModel(), self._p)
        msg_task.call()
        self.assertFalse(accessor.exists(message_key))
        current_state_key = keybuilder.current_state(
            participant_id, session_id, resource)
        self.assertTrue(accessor.exists(current_state_key))
        current_state = accessor.get(current_state_key)
        p_state = current_state['mapFields'][partition]['CURRENT_STATE']
        p_statemodel = current_state['simpleFields']['STATE_MODEL_DEF']
        p_session = current_state['simpleFields']['SESSION_ID']
        self.assertTrue(p_state == 'ERROR')
        self.assertTrue(p_statemodel == state_model_def)
        self.assertTrue(p_session == session_id)
        error_key = keybuilder.error(
            participant_id, session_id, resource, partition)
        self.assertTrue(accessor.exists(error_key))
        error_node = accessor.get(error_key)
        self.assertTrue('ERROR' in error_node['simpleFields'])
