import unittest

import pyhelix.statemodel as statemodel


class MockStateModel(statemodel.StateModel):
    existing_invoked = False
    default_invoked = False

    def on_become_online_from_offline(self, message):
        self.existing_invoked = True

    def default_transition_handler(self, message):
        self.default_invoked = True


class TestStateModelParser(unittest.TestCase):
    """
    Make sure that functions are invoked properly based on transition messages.
    """

    def setUp(self):
        self._state_model = MockStateModel()
        self._parser = statemodel.StateModelParser()

    def test_normal(self):
        """
        Normal case: all caps states invokes defined method
        """
        method = self._parser.get_method_for_transition(
            self._state_model, 'OFFLINE', 'ONLINE')
        method(None)
        self.assertTrue(self._state_model.existing_invoked)

    def test_default(self):
        """
        Defined method doesn't exist, so invoke the default
        """
        method = self._parser.get_method_for_transition(
            self._state_model, 'GOOFY', 'SILLY')
        method(None)
        self.assertTrue(self._state_model.default_invoked)

    def test_case_insensitive(self):
        """
        Invoke the correct method even when casing is weird
        """
        method = self._parser.get_method_for_transition(
            self._state_model, 'offliNE', 'onLIne')
        method(None)
        self.assertTrue(self._state_model.existing_invoked)
