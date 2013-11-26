from pyhelix import statemodel

class QueueStateModel(statemodel.StateModel):
    """
    State model for an online-offline queue set
    """
    def on_become_online_from_offline(self, message):
        partition_name = message['simpleFields']['PARTITION_NAME']
        print('{0}: on become online from offline'.format(partition_name))

    def on_become_offline_from_online(self, message):
        partition_name = message['simpleFields']['PARTITION_NAME']
        print('{0}: on become offline from online'.format(partition_name))

    def on_become_dropped_from_offline(self, message):
        partition_name = message['simpleFields']['PARTITION_NAME']
        print('{0}: on become dropped from offline'.format(partition_name))

class QueueStateModelFactory(statemodel.StateModelFactory):
    """
    State model factory for an online-offline queue set
    """
    def create_state_model(self, partition_name):
        """
        Create a QueueStateModel

        Args:
            partition_name: The partition

        Returns:
            QueueStateModel
        """
        state_model = QueueStateModel()
        return state_model
