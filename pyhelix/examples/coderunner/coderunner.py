import argparse
import bottle
import logging
import os
import random
import re
import urllib
import urllib2

import pyhelix.spectator as spectator


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
        self._app.run(host='0.0.0.0', port=self._port)

    def stop(self):
        """
        Stop the CodeRunner
        """
        self._conn.disconnect()

    def show_index(self):
        """
        Called when the home page is requested

        Returns:
            A page with a box to input code where to run it
        """
        participants = self._s.get_participants('ONLINE')
        participants = [p['id'] for p in participants]
        return bottle.template(
            'index', host=self._host, port=self._port,
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
        outputs = self._run_on_nodes(prog, pattern)
        return bottle.template('result', results=outputs, prog=prog)

    def static_files(self, filename):
        """
        Static route to files

        Args:
            filename: name of the static file

        Returns:
            a file handle
        """
        path = os.path.dirname(os.path.realpath(__file__)) + '/static'
        return bottle.static_file(filename, root=path)

    def _run_on_nodes(self, prog, pattern):
        """
        Run a program on all machines that match a pattern.

        Args:
            prog: The text of the program
            pattern: The machine pattern

        Returns:
            The aggregate output
        """
        outputs = []
        pattern = re.compile(pattern, re.IGNORECASE)
        participants = self._s.get_participants('ONLINE')
        for participant in participants:
            if pattern.match(participant['id']):
                label = participant['id']
                host = participant['simpleFields']['HELIX_HOST']
                port = participant['simpleFields']['HELIX_PORT']
                values = {'prog': prog}
                data = urllib.urlencode(values)
                result = urllib2.urlopen(
                    'http://{0}:{1}/run'.format(host, port), data)
                output = label, result.read()
                outputs.append(output)
        return outputs

    def _route(self):
        """
        Route HTTP requests (private)
        """
        self._app.route('/', callback=self.show_index)
        self._app.route('/run', method='POST', callback=self.run_program)
        self._app.route('/static/<filename>', callback=self.static_files)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--zkSvr', required=True, type=str, help='host:port of ZooKeeper')
    parser.add_argument('--host', required=True, type=str, help='hostname')
    parser.add_argument('--port', required=True, type=str, help='port')
    args = parser.parse_args()
    r = CodeRunner(
        'coderunner-cluster', 'coderunner', args.host, args.port, args.zkSvr)
    r.start()
    r.stop()
