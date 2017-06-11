============================================
Pserv : mega-scale undistributed database
============================================

This is a package of scripts to set up, maintain, and provide interfaces to a
very simple "preliminary" or "practice" database which serves LSST data resulting from
running early versions of the Level 1 and Level 2 pipelines.  A
subset of the tables using the LSST `baseline
schema <https://lsst-web.ncsa.illinois.edu/schema/index.php?sVer=baseline>`_
will be supported.

The production LSST database will be implemented via
`Qserv <https://github.com/lsst/qserv>`_, which is a distributed
database implementation with a "MySQL-like" DBMS.  Since
``Pserv`` is meant to emulate ``Qserv`` but on a single node, a MySQL
database is used. ``Pserv`` allows you to start designing and testing
queries on LSST catalogs while ``Qserv`` is still being developed.



API Documentation
=================

Pserv
-----

The main `PServ` module contains classes for building a `Pserv`
mega-scale undistributed database: handling the connection to the
`mysql` database, and transferring in data from files. If no
documentation for the `Pserv` functions appears below (and before the
"Repository Info" section), it means that the Sphinx `conf.py` needs
fixing to handle the various import statements better.

.. automodule:: Pserv
    :members:
    :undoc-members:


Repository Info
---------------

.. automodule:: repository_info
    :members:
    :undoc-members:


Registry Tools
--------------

.. automodule:: registry_tools
    :members:
    :undoc-members:


Utilities
---------

If no documentation for the utility functions appears below it means
that the Sphinx `conf.py` needs fixing to handle the various import
statements better.

.. automodule:: utils
    :members:
    :undoc-members:
