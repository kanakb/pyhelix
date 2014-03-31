import pyhelix.statemodel as statemodel


class CodeRunnerModel(statemodel.StateModel):
    """
    State model for an online-offline code runner set
    """
    def __init__(self):
        """
        Initialize a CodeRunnerModel
        """
        self._active = False

    def is_active(self):
        """
        Check if requests should be actively served

        Returns:
            True if online, False if offline
        """
        return self._active

    def on_become_online_from_offline(self, message):
        partition_name = message['simpleFields']['PARTITION_NAME']
        self._active = True
        print('{0}: on become online from offline'.format(partition_name))

    def on_become_offline_from_online(self, message):
        partition_name = message['simpleFields']['PARTITION_NAME']
        self._active = False
        print('{0}: on become offline from online'.format(partition_name))

    def on_become_dropped_from_offline(self, message):
        partition_name = message['simpleFields']['PARTITION_NAME']
        print('{0}: on become dropped from offline'.format(partition_name))

    def on_become_offline_from_error(self, message):
        partition_name = message['simpleFields']['PARTITION_NAME']
        self._active = False
        print('{0}: on become offline from error'.format(partition_name))


class CodeRunnerModelFactory(statemodel.StateModelFactory):
    """
    State model factory for an online-offline code runner set
    """
    def create_state_model(self, partition_name):
        """
        Create a CodeRunnerModel

        Args:
            partition_name: The partition

        Returns:
            CodeRunnerModel
        """
        return CodeRunnerModel()
