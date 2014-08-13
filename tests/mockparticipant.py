import pyhelix.accessor as accessor
import pyhelix.participant as participant

import mockclient


class MockParticipant(participant.Participant):
    """
    A participant that can't do anything substantial
    """
    def __init__(self, cluster_id, host, port, zk_addrs):
        participant.Participant.__init__(
            self, cluster_id, host, port, zk_addrs)
        self._client = mockclient.MockKazooClient()
        self._accessor = accessor.DataAccessor(cluster_id, self._client)

    def connect(self):
        self._is_lost = False
        self._client.start()
