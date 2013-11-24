from concurrent import futures
import logging

import helixtask
import znode

class HelixExecutor(object):
    """
    Helix executor that listens on transition messages and schedule transition tasks
    """

    def __init__(self, state_model_ftys, participant):
        """
        Initialize the executor

        Args:
            state_model_ftys: An iterable collection of state model factories
            participant: A Helix participant object
        """
        self._threadpool = futures.ThreadPoolExecutor(1)
        self._state_model_ftys = state_model_ftys
        self._accessor = participant.get_accessor()
        self._builder = self._accessor.get_key_builder()
        self._participant_id = participant.get_participant_id()
        self._participant = participant

    def on_message(self, messages):
        """
        Process incoming messages.

        Args:
            messages: Message ZNodes
        """
        for message in messages:
            if not message or not message['simpleFields'] or not message['simpleFields']['MSG_TYPE']:
                continue
            if message['simpleFields']['MSG_TYPE'].upper() != 'STATE_TRANSITION':
                continue
            if message['simpleFields']['MSG_STATE'].upper() != 'NEW':
                continue
            state_model_name = message['simpleFields']['STATE_MODEL_DEF']
            state_model_fty = self._state_model_ftys[state_model_name]

            partition_name = message['simpleFields']['PARTITION_NAME']
            state_model = state_model_fty.get_state_model(partition_name)
            if state_model == None:
                state_model = state_model_fty.create_state_model(partition_name)
                state_model_fty.put_state_model(partition_name, state_model)

            task = helixtask.HelixTask(message, state_model, self._participant)
            self._threadpool.submit(task.call)

        # update messages to read
        for message in messages:
            if not message or not message['simpleFields']:
                continue
            message['simpleFields']['MSG_STATE'] = 'READ'
            self._accessor.set(self._builder.message(self._participant_id, message['id']), message)
