import argparse
import bottle
import cStringIO
import logging
import sys
import time
import traceback

import pyhelix.participant as participant

import runnermodel

logging.basicConfig(level=logging.WARN)


class CodeRunnerProcess(object):
    """
    A class that makes a node that runs code available
    """
    def __init__(self, cluster, host, port, zk_svr):
        """
        Initialize the CodeRunnerProcess

        Args:
            cluster: the cluster id
            host: the hostname that this runner can be reached on
            port: the port to use to send data
            zk_svr: host:port of a ZooKeeper server
        """
        self._p = participant.Participant(cluster, host, port, zk_svr)
        self._factory = runnermodel.CodeRunnerModelFactory()
        self._p.register_state_model_fty(
            'OnlineOfflineModified', self._factory)
        self._app = bottle.Bottle()
        self._host = host
        self._port = port
        self._route()

    def start(self):
        """
        Start the CodeRunnerProcess
        """
        self._p.connect()
        self._app.run(host='0.0.0.0', port=self._port)

    def stop(self):
        """
        Stop the CodeRunnerProcess
        """
        self._p.disconnect()

    def run_program(self):
        """
        Called when a request with a program to run comes in

        Returns:
            stdout result of running the program
        """
        logging.info('New request to {0}:{1}'.format(self._host, self._port))
        bottle.response.add_header('Content-Type', 'text/plain')
        if not self._can_serve():
            return '[ERROR] Not accepting program submissions\n'
        prog = str(bottle.request.forms.get('prog'))
        old_stdout = sys.stdout
        redirected_output = sys.stdout = cStringIO.StringIO()
        try:
            exec(prog)
        except:
            print traceback.format_exc()
        sys.stdout = old_stdout
        return redirected_output.getvalue()

    def _can_serve(self):
        """
        Check if this machine should serve requests

        Returns:
            True if requests are accepted, False otherwise
        """
        model = self._factory.get_state_model('coderunner_0')
        if model.is_active():
            return True
        else:
            return False

    def _route(self):
        """
        Route HTTP requests (private)
        """
        self._app.route('/run', method='POST', callback=self.run_program)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--zkSvr', required=True, type=str, help='host:port of ZooKeeper')
    parser.add_argument('--host', required=True, type=str, help='hostname')
    parser.add_argument('--port', required=True, type=str, help='port')
    args = parser.parse_args()
    r = CodeRunnerProcess(
        'coderunner-cluster', args.host, args.port, args.zkSvr)
    r.start()
    r.stop()
