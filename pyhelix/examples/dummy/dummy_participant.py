import argparse
import logging
import threading

from pyhelix import participant
from pyhelix import statemodel

logging.basicConfig(level=logging.WARN)

# This is a dummy participant. It starts up, runs forever, and prints state transitions as it gets
# them. It's close to what a real participant would look like, except that it uses a
# MockStateModel.

def main(args):
    p = participant.Participant(args.cluster, args.host, args.port, args.zkSvr)
    try:
        p.register_state_model_fty('MasterSlave', statemodel.MockStateModelFactory())
        p.connect()

        # wait forever
        dummy_event = threading.Event()
        dummy_event.wait()
    finally:
        p.disconnect()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--cluster', required=True, type=str, help='Name of the cluster')
    parser.add_argument('--zkSvr', required=True, type=str, help='host:port of ZooKeeper')
    parser.add_argument('--host', required=True, type=str, help='hostname')
    parser.add_argument('--port', required=True, type=str, help='port')
    main(parser.parse_args())
