# Pserv: megascale undistributed database

This is a package of scripts to maintain and provide interfaces to a
"preliminary" or "practice" database which serves data resulting from
running early versions of the LSST Level 1 and Level 2 pipelines.  A
subset of the tables using the LSST [baseline
schema](https://lsst-web.ncsa.illinois.edu/schema/index.php?sVer=baseline)
will be supported.

The production LSST database will be implemented via
[Qserv](https://github.com/lsst/qserv), which is a distributed
database implementation, and which uses a "MySQL-like" DBMS.  Since
`Pserv` is meant to emulate `Qserv` but runs on a single node, a MySQL
database is used.

## People
* [Jim Chiang](https://github.com/DarkEnergyScienceCollaboration/Pserv/issues/new?body=@jchiang87) (SLAC)

## License, etc.

This is open source software, available under the BSD license. If you are interested in this project, please do drop us a line via the hyperlinked contact names above, or by [writing us an issue](https://github.com/DarkEnergyScienceCollaboration/Pserv/issues/new).
