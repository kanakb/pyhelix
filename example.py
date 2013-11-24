import kazoo.client
import logging
import time

from pyhelix import participant
from pyhelix import statemodel

logging.basicConfig(level=logging.WARN)

p = participant.Participant('perf-test-cluster', 'localhost', 12120, 'eat1-app87.corp:2181')
try:
    p.register_state_model_fty('MasterSlave', statemodel.MockStateModelFactory())
    p.connect()

    time.sleep(30000000)
finally:
    p.disconnect()
