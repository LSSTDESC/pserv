#!/usr/bin/env python
"""
Run all of the test scripts with filename pattern test_*.py.
"""
from __future__ import absolute_import
import os
import unittest
runner = unittest.TextTestRunner()
loader = unittest.TestLoader()
testsuite = loader.discover(os.path.join(os.environ['PSERV_DIR'], 'tests'),
                            pattern='test_*.py')
runner.run(testsuite)
