import kazoo.client
import kazoo.exceptions


class MockStruct(object):
    """
    Basic object to allow adding arbitrary properties
    """


class MockKazooClient(object):
    """
    In-memory version of the kazoo client for unit testing
    """

    def __init__(self):
        self.store = {'/': None}
        self.ephemerals = set()
        self.versions = {}
        self._connected = False

    def start(self):
        self._connected = True

    def stop(self):
        for eph in self.ephemerals:
            self.store.pop(eph)
        self._connected = False

    @property
    def connected(self):
        return self._connected

    @property
    def client_id(self):
        return 123, 'password'

    def restart(self):
        self.stop()
        self.start()

    def close(self):
        pass

    def create(self, path, data, ephemeral=False, sequence=False,
               makepath=False):
        # TODO: sequence not implemented
        if path in self.store or path == '/':
            raise kazoo.exceptions.NodeExistsError
        parts = path.split('/')
        for i in xrange(1, len(parts)):
            subpath = '/'.join(parts[:i])
            if not subpath:
                subpath = '/'
            if subpath in self.store and subpath in self.ephemerals:
                raise kazoo.exceptions.NoChildrenForEphemeralsError
            if subpath not in self.store:
                if makepath or subpath == '/':
                        self.store[subpath] = None
                else:
                    raise kazoo.exceptions.NoNodeError
        if ephemeral:
            self.ephemerals.add(path)
        self.store[path] = data
        return path

    def ensure_path(self, path):
        if path in self.store or path == '/':
            return
        parts = path.split('/')
        for i in xrange(1, len(parts)):
            subpath = '/'.join(parts[:i])
            if subpath not in self.store:
                self.store[subpath] = None
        self.store[path] = None

    def exists(self, path):
        return path if path in self.store else None

    def get(self, path):
        # TODO: manage versions internally better
        if path not in self.store:
            raise kazoo.exceptions.NoNodeError
        get_stat = MockStruct()
        get_stat.version = -1
        return self.store[path], True

    def get_children(self, path, include_data=False):
        # TODO: include_data doesn't do the right thing
        if path not in self.store:
            raise kazoo.exceptions.NoNodeError
        parts = path.split('/')
        result = []
        for existpath, data in self.store.iteritems():
            if existpath.startswith(path):
                existparts = existpath.split('/')
                if len(existparts) == len(parts) + 1:
                    if include_data:
                        result.append((existpath, data))
                    else:
                        result.append(existpath)
        return result

    def set(self, path, data, version=-1):
        if path not in self.store:
            raise kazoo.exceptions.NoNodeError
        if path in self.versions:
            existversion = self.versions[path]
            if version != -1 and existversion > version:
                raise kazoo.exceptions.BadVersionError
        self.store[path] = data
        self.versions[path] = version

    def delete(self, path, version=-1, recursive=False):
        if path not in self.store:
            raise kazoo.exceptions.NoNodeError
        existversion = -1
        if path in self.versions:
            existversion = self.versions[path]
            if version != -1 and existversion > version:
                raise kazoo.exceptions.BadVersionError
        to_pop = []
        for existpath in self.store.iterkeys():
            if existpath.startswith(path):
                if path != existpath and not recursive:
                    raise kazoo.exceptions.NotEmptyError
                to_pop.append(existpath)
        for subpath in to_pop:
            self.store.pop(subpath)
            if subpath in self.versions:
                self.versions.pop(subpath)
            if subpath in self.ephemerals:
                self.ephemerals.pop(subpath)
        if path == '/':
            self.store = {'/': None}

    def add_listener(self, unused):
        pass

    def remove_listener(self, unused):
        pass
