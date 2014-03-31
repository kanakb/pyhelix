import argparse
import logging
import threading

import pyhelix.participant as participant

import dummy_statemodel

logging.basicConfig(level=logging.WARN)

# This is a dummy participant. It starts up, runs forever, and prints state
# transitions as it gets them. It's close to what a real participant would look
# like, except that it uses a DummyStateModel.


def main(args):
    p = participant.Participant(
        args.cluster, args.host, args.port, args.zkSvr,
        participant_id=args.participantId)
    try:
        p.register_state_model_fty(
            'MasterSlave', dummy_statemodel.DummyStateModelFactory())
        p.connect()

        # wait forever
        dummy_event = threading.Event()
        dummy_event.wait()
    finally:
        p.disconnect()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cluster', required=True, type=str, help='Name of the cluster')
    parser.add_argument(
        '--zkSvr', required=True, type=str, help='host:port of ZooKeeper')
    parser.add_argument('--host', required=True, type=str, help='hostname')
    parser.add_argument('--port', required=True, type=str, help='port')
    parser.add_argument(
        '--participantId', required=False, type=str,
        help="Optional custom participant ID")
    main(parser.parse_args())
