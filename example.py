import kazoo.client
import logging
import time

from pyhelix import participant
from pyhelix import statemodel

logging.basicConfig(level=logging.WARN)

p = participant.Participant('cluster-name', 'myhostname', 12120, 'zkhost:2181')
try:
    p.register_state_model_fty('MasterSlave', statemodel.MockStateModelFactory())
    p.connect()

    time.sleep(30000000)
finally:
    p.disconnect()
