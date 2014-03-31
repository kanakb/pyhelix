import argparse
import logging
import threading
import time

import pyhelix.spectator as spectator

logging.basicConfig(level=logging.WARN)

# This is a dummy spectator. It connects, looks at the external view for the
# given partitions, and then exits.


def main(args):
    """
    This starts a spectator, uses it to look up partitions, then exits.
    """
    conn = spectator.SpectatorConnection(args.cluster, args.zkSvr)
    try:
        conn.connect()
        s = conn.spectate(args.resource)
        for partition_id in args.partitions:
            print s.get_state_map(partition_id)
    finally:
        conn.disconnect()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cluster', required=True, type=str, help='Name of the cluster')
    parser.add_argument(
        '--resource', required=True, type=str, help='Name of the resource')
    parser.add_argument(
        '--partitions', required=True, type=str, nargs='+',
        help='Partitions to look at')
    parser.add_argument(
        '--zkSvr', required=True, type=str, help='host:port of ZooKeeper')
    main(parser.parse_args())
