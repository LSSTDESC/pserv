# Pserv: megascale undistributed database

This is a package of scripts to set up, maintain, and provide interfaces to a
very simple "preliminary" or "practice" database which serves LSST data resulting from
running early versions of the Level 1 and Level 2 pipelines.  A
subset of the tables using the LSST [baseline
schema](https://lsst-web.ncsa.illinois.edu/schema/index.php?sVer=baseline)
will be supported.

The production LSST database will be implemented via
[`Qserv`](https://github.com/lsst/qserv), which is a distributed
database implementation with a "MySQL-like" DBMS.  Since
`Pserv` is meant to emulate `Qserv` but on a single node, a MySQL
database is used. `Pserv` allows you to start designing and testing 
queries on LSST catalogs while `Qserv` is still being developed.

## People
* [Jim Chiang](https://github.com/DarkEnergyScienceCollaboration/pserv/issues/new?body=@jchiang87) (SLAC)
* [Phil Marshall](https://github.com/DarkEnergyScienceCollaboration/pserv/issues/new?body=@drphilmarshall) (SLAC)

## License, etc.

This is open source software, available under the BSD license. If you are interested in this project, please do drop us a line via the hyperlinked contact names above, or by [writing us an issue](https://github.com/DarkEnergyScienceCollaboration/pserv/issues/new).
