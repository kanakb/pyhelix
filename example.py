import kazoo.client
import logging
import time

from pyhelix import participant
from pyhelix import statemodel

logging.basicConfig(level=logging.INFO)

p = participant.Participant('test-cluster', 'localhost', 12120, 'localhost:2199')
try:
    p.register_state_model_fty('MasterSlave', statemodel.MockStateModelFactory())
    p.connect()

    time.sleep(30000000)
finally:
    p.disconnect()
