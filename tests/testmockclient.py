import unittest
import kazoo.exceptions

import mockclient


class TestMockKazooClient(unittest.TestCase):
    """
    MockKazooClient is nontrivial, so this verifies basic correctness
    """

    def setUp(self):
        self.c = mockclient.MockKazooClient()
        self.c.start()

    def test_connected(self):
        """
        Connection state should correspond to start/stop
        """
        self.assertTrue(self.c.connected)
        self.c.stop()
        self.assertFalse(self.c.connected)

    def test_create_basic(self):
        """
        Test that a default creation is successful
        """
        path = '/testPath'
        result = self.c.create(path, 'testData')
        self.assertEqual(result, path)
        data, metadata = self.c.get(path)
        self.assertTrue(metadata is not None)
        self.assertEqual(data, 'testData')

    def test_create_recursive(self):
        """
        Test that recursive creation only passes if makepath is True
        """
        # empty set
        path = '/path/testPath'
        self.assertRaises(
            kazoo.exceptions.NoNodeError, self.c.create, path, 'testData')
        result = self.c.create(path, 'testData', makepath=True)
        self.assertEqual(result, path)

        # middle is missing
        path2 = path + '/two/more'
        self.assertRaises(
            kazoo.exceptions.NoNodeError, self.c.create, path2, 'testData2')
        result = self.c.create(path2, 'testData2', makepath=True)
        self.assertEqual(result, path2)

    def test_create_ephemeral(self):
        """
        Test ephemeral node creation behavior
        """
        path = '/testPath'
        result = self.c.create(path, 'testData', ephemeral=True)
        self.assertTrue(result, path)

        # it's not allowed to have a child of an ephemeral node
        path = path + '/child'
        self.assertRaises(
            kazoo.exceptions.NoChildrenForEphemeralsError, self.c.create, path,
            'testChildData')

    def test_ensure_path(self):
        """
        Ensure path should create the path and all subpaths
        """
        # this shouldn't raise any errors
        path = '/one/two/three/four'
        self.c.ensure_path(path)
        data, metadata = self.c.get('/one')
        self.assertTrue(metadata is not None)
        data, metadata = self.c.get('/one/two')
        self.assertTrue(metadata is not None)
        data, metadata = self.c.get('/one/two/three')
        self.assertTrue(metadata is not None)
        data, metadata = self.c.get('/one/two/three/four')
        self.assertTrue(metadata is not None)
        self.assertTrue(self.c.exists('/one'))
        self.assertTrue(self.c.exists('/one/two'))
        self.assertTrue(self.c.exists('/one/two/three'))
        self.assertTrue(self.c.exists('/one/two/three/four'))

    def test_exists(self):
        """
        Ensure that exists is correct
        """
        path = '/one/two/three/four'
        self.assertFalse(self.c.exists('/one'))
        self.c.create(path, 'testData', makepath=True)
        self.assertTrue(self.c.exists('/one'))
        self.assertTrue(self.c.exists('/one/two'))
        self.assertTrue(self.c.exists('/one/two/three'))
        self.assertTrue(self.c.exists('/one/two/three/four'))

    def test_get_nonexistent(self):
        """
        Ensure that get raises an error if the node doesn't exist
        """
        path = '/one/two/three/four'
        self.assertRaises(kazoo.exceptions.NoNodeError, self.c.get, path)

    def test_get_children(self):
        """
        Test that get children gets all children and no others
        """
        self.c.create('/one', 'onedata')
        self.c.create('/outside', 'outsidedata')
        self.c.create('/outside/a', 'outsideadata')
        self.c.create('/one/two', 'twodata')
        self.c.create('/one/three', 'threedata')
        children = self.c.get_children('/one')
        self.assertTrue('/one/two' in children)
        self.assertTrue('/one/three' in children)
        self.assertTrue('/one' not in children)
        self.assertTrue('/outside' not in children)
        self.assertTrue('/outside/a' not in children)
        self.assertEqual(len(children), 2)

    def test_basic_set(self):
        """
        Test that set updates the value
        """
        path = '/one'
        self.assertRaises(
            kazoo.exceptions.NoNodeError, self.c.set, path, 'data')
        self.c.create(path, 'data')
        self.c.set(path, 'updated')
        data, metadata = self.c.get(path)
        self.assertEqual(data, 'updated')
        self.assertTrue(metadata is not None)

    def test_set_version_mismatch(self):
        """
        Test that set behaves correctly on version match/mismatch
        """
        path = '/one'
        self.c.create(path, 'data')
        self.c.set(path, 'updated', version=1)
        data, metadata = self.c.get(path)
        self.assertEqual(data, 'updated')
        self.c.set(path, 'updated2', version=2)
        data, metadata = self.c.get(path)
        self.assertEqual(data, 'updated2')
        self.assertRaises(
            kazoo.exceptions.BadVersionError, self.c.set, path, 'updated3',
            version=1)
        data, metadata = self.c.get(path)
        self.assertNotEqual(data, 'updated3')
        self.c.set(path, 'updated4', version=-1)
        data, metadata = self.c.get(path)
        self.assertEqual(data, 'updated4')

    def test_basic_delete(self):
        """
        Test that a node is removed once delete is called
        """
        path = '/one'
        self.assertRaises(kazoo.exceptions.NoNodeError, self.c.delete, path)
        self.c.create(path, 'data')
        data, metadata = self.c.get(path)
        self.assertEqual(data, 'data')
        self.c.delete(path)
        self.assertFalse(self.c.exists(path))

    def test_recursive_delete(self):
        """
        Test that recursive delete does the right thing
        """
        self.c.create('/one', None)
        self.c.create('/one/a', None)
        self.c.create('/one/b', None)
        self.c.create('/two', None)
        self.c.create('/two/a', None)
        self.assertRaises(
            kazoo.exceptions.NotEmptyError, self.c.delete, '/one')
        self.c.delete('/one', recursive=True)
        self.assertFalse(self.c.exists('/one'))
        self.assertFalse(self.c.exists('/one/a'))
        self.assertFalse(self.c.exists('/one/b'))
        self.assertTrue(self.c.exists('/two'))
        self.assertTrue(self.c.exists('/two/a'))

    def test_delete_version_mismatch(self):
        """
        Test that delete pays attention to version
        """
        path = '/one'
        self.c.create(path, 'data')
        self.c.set(path, 'updated', version=2)
        self.assertRaises(
            kazoo.exceptions.BadVersionError, self.c.delete, path, version=1)
        self.c.delete(path, version=3)
        self.assertFalse(self.c.exists(path))
        self.c.create(path, 'data')
        self.c.set(path, 'updated', version=2)
        self.c.delete(path, version=3)

    def tearDown(self):
        self.c.stop()
        self.c.close()
