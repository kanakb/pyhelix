import concurrent.futures as futures
import logging
import time

import helixtask
import znode


class HelixExecutor(object):
    """
    Helix executor that listens on transition messages and schedules tasks
    """

    DEFAULT_PARALLELISM = 20

    def __init__(self, state_model_ftys, participant, num_concurrent=None):
        """
        Initialize the executor

        Args:
            state_model_ftys: An iterable collection of state model factories
            participant: A Helix participant object
        """
        if not num_concurrent:
            num_concurrent = self.DEFAULT_PARALLELISM
        self._threadpool = futures.ThreadPoolExecutor(num_concurrent)
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
            # Skip bad messages
            if (not message or
               not message['simpleFields'] or
               not message['simpleFields']['MSG_TYPE']):
                continue

            # Skip messages that aren't transition messages
            message_type = message['simpleFields']['MSG_TYPE'].upper()
            if message_type != 'STATE_TRANSITION':
                continue

            # Remove messages that aren't for this session
            tgt_session_id = message['simpleFields']['TGT_SESSION_ID']
            session_id = self._participant.get_session_id()
            if tgt_session_id != session_id:
                logging.warn(
                    'Message {0} has target session id {1},'
                    ' expected {2}'.format(
                        message['id'], tgt_session_id, session_id))
                self._accessor.remove(
                    self._builder.message(self._participant_id, message['id']))
                continue

            # Skip messages that are already read
            message_state = message['simpleFields']['MSG_STATE'].upper()
            if message_state != 'NEW':
                continue

            # Get the state model, instantiating if it doesn't exist
            state_model_name = message['simpleFields']['STATE_MODEL_DEF']
            state_model_fty = self._state_model_ftys[state_model_name]
            partition_name = message['simpleFields']['PARTITION_NAME']
            state_model = state_model_fty.get_state_model(partition_name)
            if state_model is None:
                state_model = state_model_fty.create_state_model(
                    partition_name)
                state_model_fty.put_state_model(partition_name, state_model)

            # Update message to READ
            message['simpleFields']['MSG_STATE'] = 'READ'
            message['simpleFields']['READ_TIMESTAMP'] = '{0}'.format(
                int(time.time() * 1000))
            message['simpleFields']['EXE_SESSION_ID'] = session_id
            self._accessor.update(self._builder.message(
                self._participant_id, message['id']), message)

            # Schedule the transition for processing
            task = helixtask.HelixTask(message, state_model, self._participant)
            self._threadpool.submit(task.call)
