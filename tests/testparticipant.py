import unittest

import pyhelix.participant as participant


class TestParticipant(unittest.TestCase):
    """
    These test methods test various parts of the participant lifecycle
    """

    def test_derived_id(self):
        """
        Test that a participant id is derived when none is specified
        """
        host = 'localhost'
        port = 123
        p = participant.Participant(
            'test-cluster', host, port, 'localhost:2181')
        self.assertEqual(p._participant_id, '{0}_{1}'.format(host, port))

    def test_custom_id(self):
        """
        Test that a custom participant id is honored
        """
        host = 'localhost'
        port = 123
        participant_id = 'myid'
        p = participant.Participant(
            'test-cluster', host, port, 'localhost:2181',
            participant_id=participant_id)
        self.assertEqual(p._participant_id, participant_id)
