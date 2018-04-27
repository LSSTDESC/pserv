"""
Unit tests for the RepositoryInfo class.
"""
from __future__ import absolute_import, print_function
import os
import unittest
from warnings import filterwarnings
import lsst.utils
import desc.pserv

filterwarnings('ignore')

def _test_dir_path(x):
    return os.path.join(lsst.utils.getPackageDir('pserv'), 'tests', x)

class RepositoryInfoTestCase(unittest.TestCase):
    """
    TestCase for RepositoryInfo class.
    """
    def setUp(self):
        self.registry_file = 'registry.sqlite3'
        self.repo_info = desc.pserv.RepositoryInfo(_test_dir_path('output'))

    def tearDown(self):
        pass

    def test_find_registry(self):
        registry_path = \
            self.repo_info.find_registry(self.repo_info.repo, self.registry_file)
        self.assertTrue(
            os.path.samefile(os.path.join(_test_dir_path('image_repo'), self.registry_file),
                             registry_path))

    def test_get_visit_mjds(self):
        mjds = self.repo_info.get_visit_mjds()
        mjd_keys = [key for key in mjds.keys()]
        mjd_values = [value for value in mjds.values()]
        self.assertEqual(mjd_keys[0], 921297)
        self.assertAlmostEqual(mjd_values[0], 60934.288509722224)
        self.assertEqual(mjd_keys[-1], 1973403)
        self.assertAlmostEqual(mjd_values[-1], 62497.068709722225)

    def test_get_visits(self):
        visits = self.repo_info.get_visits()
        self.assertEqual(len(visits['u']), 0)
        self.assertEqual(len(visits['r']), 5)
        self.assertTrue(921297 in visits['r'])
        self.assertTrue(1414156 in visits['r'])

    def test_get_sensors(self):
        sensors = self.repo_info.get_sensors()
        self.assertTrue(('2,2', '0,0') in sensors)
        self.assertTrue(('2,2', '2,2') in sensors)
        self.assertFalse(('2,1', '0,1') in sensors)

    def test_get_patches(self):
        patches = self.repo_info.get_patches()
        keys = [key for key in patches.keys()]
        self.assertSequenceEqual((0,), keys)
        self.assertEqual(len(patches[0]), 9)
        self.assertTrue('0,0' in patches[0])
        self.assertTrue('1,1' in patches[0])
        self.assertTrue('2,1' in patches[0])
        self.assertFalse('3,0' in patches[0])

if __name__ == '__main__':
    unittest.main()
