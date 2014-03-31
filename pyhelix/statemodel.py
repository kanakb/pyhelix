import logging


class StateModelParser(object):
    """
    State model helper
    """

    def get_method_for_transition(self, clazz, from_state, to_state):
        """
        Get the callback for a state get_method_for_transition

        Args:
            clazz: The class that extends StateModel with the callbacks
            from_state: The previous state
            to_state: The new state

        Returns:
            The method to call
        """
        method_name = (
            'on_become_' + to_state.lower() + '_from_' + from_state.lower())
        logging.debug('method_name: {0}'.format(method_name))
        return getattr(clazz, method_name, clazz.default_transition_handler)


class StateModel(object):
    """
    Base state model
    """

    DEFAULT_INIT_STATE = 'OFFLINE'

    def __init__(self):
        """
        Initialize the basic state model
        """
        self._current_state = StateModel.DEFAULT_INIT_STATE

    def get_current_state(self):
        """
        Get the current state for this partition

        Returns:
            Current state string for this partition
        """
        return self._current_state

    def default_transition_handler(self, message):
        """
        Default method for when no method is available to handle the transition

        Args:
            message: the transition message
        """
        from_state = message['simpleFields']['FROM_STATE']
        to_state = message['simpleFields']['TO_STATE']
        logging.warn(
            'No method found for {0}-{1}'.format(from_state, to_state))


class StateModelFactory(object):
    """
    Base state model factory
    """

    def __init__(self):
        """
        Initialize the factory
        """
        self._state_models = {}

    def create_state_model(self, partition_name):
        """
        create a new state model (implemented by subclasses)
        """
        pass

    def put_state_model(self, partition_name, state_model):
        """
        Associate a partition with a state model

        Args:
            partition_name: The partition
            state_model: The state model
        """
        self._state_models[partition_name] = state_model

    def get_state_model(self, partition_name):
        """
        Get the associated state model for this partition

        Args:
            partition_name: The partition

        Returns:
            The state model, or None
        """
        if partition_name in self._state_models:
            return self._state_models[partition_name]
        return None
