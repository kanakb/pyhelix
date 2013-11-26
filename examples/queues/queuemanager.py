import argparse
import bottle
import logging
import Queue
import time

from pyhelix import participant

import queuemodel

logging.basicConfig(level=logging.WARN)

class QueueManager(class):
    """
    A class that makes queues available.
    """
    def __init__(self, cluster, host, port, zkSvr):
        """
        Initialize the QueueManager

        Args:
            cluster: the cluster id
            host: the hostname that these queues can be reached on
            port: the port to use to send data to queues
            zkSvr: host:port of a ZooKeeper server
        """
        self._p = participant.Participant(cluster, host, port, zkSvr)
        self._p.register_state_model_fty('OnlineOffline', queuemodel.QueueStateModelFactory())
        self._app = bottle.Bottle()
        self._host = host
        self._port = port
        self._route()
        self._queues = {}

    def start(self):
        """
        Start the QueueManager
        """
        self._p.connect()

    def stop(self):
        """
        Stop the QueueManager
        """
        self._p.disconnect()

    def show_queues(self):
        return self._queues

    def show_queue(self, qid):
        if qid in self._queues:
            return self._queues[qid]
        return None

    def put(self, qid, value):
        q = None
        if qid in self._queues:
            q = self._queues[qid]
        else:
            q = Queue.Queue()
            self._queues[qid] = q
        q.put(value)

    def take(self, qid, value):
        if qid in self._queues:
            q = self._queues[qid]
            if not q.empty():
                return q.get()
        return None

    def _route(self):
        self._app.route('/', callback=self.show_queues)
        self._app.route('/show/<qid>', callback=self.show_queue)
        self._app.route('/put/<qid>/<value>', callback=self.put_value_on_queue)
        self._app.route('/take/<qid>/<value>', callback=self.take_value_off_queue)

def main(args):
    """
    Start a single QueueManager participant
    """
    proc = QueueManager(args.cluster, args.host, args.port, args.zkSvr)
    try:
        proc.start()
        time.sleep(30000000)
    finally:
        proc.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--cluster', required=True, type=str, help='Name of the cluster')
    parser.add_argument('--zkSvr', required=True, type=str, help='host:port of ZooKeeper')
    parser.add_argument('--host', required=True, type=str, help='hostname')
    parser.add_argument('--port', required=True, type=str, help='port')
    main(parser.parse_args())
