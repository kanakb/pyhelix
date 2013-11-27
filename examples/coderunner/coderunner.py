import argparse
import bottle
import logging
import random
import re
import urllib
import urllib2

from pyhelix import spectator

class CodeRunner(object):
    """
    A class that will find nodes that run code and dispatch work
    """
    def __init__(self, cluster, resource, host, port, zk_svr):
        """
        Initialize the CodeRunner

        Args:
            cluster: the cluster id
            resource: the resource id
            host: the hostname that this router can be reached on
            port: the port to use to send data
            zk_svr: host:port of a ZooKeeper server
        """
        self._conn = spectator.SpectatorConnection(cluster, zk_svr)
        self._s = None
        self._resource = resource
        self._app = bottle.Bottle()
        self._host = host
        self._port = port
        self._route()

    def start(self):
        """
        Start the CodeRunner
        """
        self._conn.connect()
        self._s = self._conn.spectate(self._resource)
        self._app.run(host=self._host, port=self._port)

    def stop(self):
        """
        Stop the CodeRunner
        """
        self._conn.disconnect()

    def show_index(self):
        """
        Called when the home page is requested

        Returns:
            An HTML page with a box to input code and a menu to select where to run it
        """
        participants = self._s.get_participants('ONLINE')
        participants = [p['id'] for p in participants]
        return bottle.template('index', host=self._host, port=self._port,
            participants=participants)

    def run_program(self):
        """
        Called when a request with a program to run comes in

        Returns:
            stdout result of running the program
        """
        prog = str(bottle.request.forms.get('prog'))
        node = str(bottle.request.forms.get('participant'))
        logging.info('Selected participant(s): {0}'.format(node))
        pattern = node
        if node == 'star':
            pattern = '.*'
        elif node == 'random':
            participants = self._s.get_participants('ONLINE')
            if len(participants) > 0:
                pattern = random.sample(participants, 1)[0]['id']
        bottle.response.add_header('Content-Type', 'text/plain')
        return self._run_on_nodes(prog, pattern)

    def _run_on_nodes(self, prog, pattern):
        """
        Run a program on all machines that match a pattern.

        Args:
            prog: The text of the program
            pattern: The machine pattern

        Returns:
            The aggregate output
        """
        output = ''
        pattern = re.compile(pattern, re.IGNORECASE)
        participants = self._s.get_participants('ONLINE')
        for participant in participants:
            if pattern.match(participant['id']):
                output += 'Output from {0}:\n'.format(participant['id'])
                host = participant['simpleFields']['HELIX_HOST']
                port = participant['simpleFields']['HELIX_PORT']
                values = {'prog': prog}
                data = urllib.urlencode(values)
                result = urllib2.urlopen('http://{0}:{1}/run'.format(host, port), data)
                output += result.read() + '\n\n'
        return output

    def _route(self):
        """
        Route HTTP requests (private)
        """
        self._app.route('/', callback=self.show_index)
        self._app.route('/run', method='POST', callback=self.run_program)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--zkSvr', required=True, type=str, help='host:port of ZooKeeper')
    parser.add_argument('--host', required=True, type=str, help='hostname')
    parser.add_argument('--port', required=True, type=str, help='port')
    args = parser.parse_args()
    r = CodeRunner('coderunner-cluster', 'coderunner', args.host, args.port, args.zkSvr)
    r.start()
    r.stop()
