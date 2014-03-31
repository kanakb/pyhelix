import pyhelix.statemodel as statemodel


class DummyStateModel(statemodel.StateModel):
    """
    Dummy state model for master-slave
    """

    def on_become_slave_from_offline(self, message):
        partition_name = message['simpleFields']['PARTITION_NAME']
        print('{0}: on become slave from offline'.format(partition_name))

    def on_become_offline_from_slave(self, message):
        partition_name = message['simpleFields']['PARTITION_NAME']
        print('{0}: on become offline from slave'.format(partition_name))

    def on_become_master_from_slave(self, message):
        partition_name = message['simpleFields']['PARTITION_NAME']
        print('{0}: on become master from slave'.format(partition_name))

    def on_become_slave_from_master(self, message):
        partition_name = message['simpleFields']['PARTITION_NAME']
        print('{0}: on become slave from master'.format(partition_name))

    def on_become_dropped_from_offline(self, message):
        partition_name = message['simpleFields']['PARTITION_NAME']
        print('{0}: on become dropped from offline'.format(partition_name))


class DummyStateModelFactory(statemodel.StateModelFactory):
    """
    Dummy state model factory
    """

    def create_state_model(self, partition_name):
        """
        Create a DummyStateModel

        Args:
            partition_name: The partition

        Returns:
            DummyStateModel
        """
        state_model = DummyStateModel()
        return state_model
