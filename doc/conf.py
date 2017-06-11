import sys
import os

# Provide path to the python modules we want to run autodoc on
sys.path.insert(0, os.path.abspath('../python/desc/pserv'))

# Avoid imports that may be unsatisfied when running sphinx, see:
# http://stackoverflow.com/questions/15889621/sphinx-how-to-exclude-imports-in-automodule#15912502
autodoc_mock_imports = ["lsst.daf.persistence", "astropy.time",
                        "astropy.io.fits", "lsst.afw.math",
                        "lsst.utils",
                        "Pserv.create_csv_file_from_fits"]

# For reference, here's the current list of imports:
# from __future__ import absolute_import, print_function
# import copy
# import csv
# from collections import OrderedDict
# import numpy as np
# import pandas as pd
# import astropy.io.fits as fits
# import sqlalchemy
# import lsst.daf.persistence as dp
# import os
# import sqlite3
# import astropy.time
# import pickle
# import itertools
# import sys
# import lsst.afw.math as afwMath
# import lsst.utils as lsstUtils
# from .Pserv import create_csv_file_from_fits

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.mathjax',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode']

# on_rtd is whether we are on readthedocs.org, this line of code grabbed from docs.readthedocs.org
on_rtd = os.environ.get('READTHEDOCS', None) == 'True'

if not on_rtd:
    # only import and set the theme if we're building docs locally
    import sphinx_rtd_theme
    html_theme = 'sphinx_rtd_theme'
    html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

# otherwise, readthedocs.org uses their theme by default, so
# no need to specify it.

master_doc = 'index'
autosummary_generate = True
autoclass_content = "class"
autodoc_default_flags = ["members", "no-special-members"]

html_sidebars = { '**': ['globaltoc.html', 'relations.html', 'sourcelink.html', 'searchbox.html'], }

project = u'pserv'
author = u'Jim Chiang'
copyright = u'2017, ' + author
version = "0.1"
release = "0.1.0"
