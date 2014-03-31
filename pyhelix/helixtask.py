import logging
import threading

import statemodel
import znode


class HelixTask(object):
    """
    Helix task for state transitions
    """

    def __init__(self, message, state_model, participant):
        """
        Instantiate this task

        Args:
            message: The message to be processed
            state_model: The state model that the participant follows
            participant: Participant connection
        """
        self._message = message
        self._state_model = state_model
        self._state_model_parser = statemodel.StateModelParser()
        self._accessor = participant.get_accessor()
        self._builder = self._accessor.get_key_builder()
        self._participant_id = participant.get_participant_id()
        self._participant = participant

    def call(self):
        """
        Process the message.

        Calls application callbacks and then updates the global state.
        """
        thread = threading.current_thread()
        logging.info('{0} invokes message: {1}'.format(
            str(thread), self._message))
        from_state = self._message['simpleFields']['FROM_STATE']
        to_state = self._message['simpleFields']['TO_STATE']
        method_to_invoke = self._state_model_parser.get_method_for_transition(
            self._state_model, from_state, to_state)
        logging.info('method_to_invoke: {0}'.format(str(method_to_invoke)))
        method_to_invoke(self._message)
        self._state_model._current_state = to_state

        # update current-state, then remove message
        sub = False
        resource_name = self._message['simpleFields']['RESOURCE_NAME']
        partition_name = self._message['simpleFields']['PARTITION_NAME']
        current_state = znode.get_empty_znode(resource_name)
        current_state['mapFields'][partition_name] = {}
        if to_state != 'DROPPED':
            # update the current state
            current_state['mapFields'][partition_name]['CURRENT_STATE'] = (
                to_state)
            current_state['simpleFields']['STATE_MODEL_DEF'] = (
                self._message['simpleFields']['STATE_MODEL_DEF'])
            current_state['simpleFields']['SESSION_ID'] = (
                self._participant.get_session_id())
        else:
            # drop the partition from the current state
            sub = True
        self._accessor.update(self._builder.current_state(
            self._participant_id, self._participant.get_session_id(),
            resource_name), current_state, sub=sub)
        self._accessor.remove(self._builder.message(
            self._participant_id, self._message['id']))
