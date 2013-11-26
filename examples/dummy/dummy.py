import argparse
import kazoo.client
import logging
import time

from pyhelix import participant
from pyhelix import statemodel

logging.basicConfig(level=logging.WARN)

def main(args):
    p = participant.Participant(args.cluster, args.host, args.port, args.zkSvr)
    try:
        p.register_state_model_fty('MasterSlave', statemodel.MockStateModelFactory())
        p.connect()

        time.sleep(30000000)
    finally:
        p.disconnect()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--cluster', required=True, type=str, help='Name of the cluster')
    parser.add_argument('--zkSvr', required=True, type=str, help='host:port of ZooKeeper')
    parser.add_argument('--host', required=True, type=str, help='hostname')
    parser.add_argument('--port', required=True, type=str, help='port')
    main(parser.parse_args())
