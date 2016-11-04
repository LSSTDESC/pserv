## Code installation

You'll need both the `lsst_apps` and `lsst_sims` components of the
Stack installed, version v12_0 or later.  Assuming both of those are
set up via `eups`, clone the `pserv` repository from GitHub, declare
it with `eups`, and set it up:
```
$ git clone git@github.com:DarkEnergyScienceCollaboration/pserv.git
$ cd pserv
$ eups declare -r . pserv -t current
$ setup pserv
```

## Database credentials

`pserv` uses the same mechanism as the `lsst_sims` code for connecting
to a database server.  In order to connect to the MySQL instance that
the `pserv` scripts would access, you'll need to put the appropriate
credentials in your `$HOME/.lsst/db-auth.paf` file.  For connecting to
the `DESC_Twinkles_Level_2` database at NSERC, the `db-auth.paf` file
would look like
```
database: {
    authInfo: {
        host: scidb1.nersc.gov
        port: 3306
        user: desc_user
        password: <password>
    }
}
```
Here `desc_user` is the userid for the read-only account.  There is
also an admin account that can be used for writing to the tables.  The
credentials for either of these accounts can be obtained by emailing
someone on the
[Twinkles](https://github.com/DarkEnergyScienceCollaboration/Twinkles)
team.  Note that both the `~/.lsst` directory and the `db-auth.paf` must
have owner-only permissions set.

## Loading Level 2 data into the tables

Given an output repository filled with the results of Level 2
processing, the `load_db.py` script can be used to fill the MySQL
tables:
```
$ load_db.py --help
usage: load_db.py [-h] [--database DATABASE] [--host HOST] [--port PORT]
                  [--dry_run]
                  repo project

Script to create and load CcdVisit, Object, and Forced Source tables with
Level 2 pipeline output.

positional arguments:
  repo                 Repository with Stack output
  project              DESC project name

optional arguments:
  -h, --help           show this help message and exit
  --database DATABASE  Database to use
  --host HOST          Host server for the MySQL database
  --port PORT          Port used by the database host
  --dry_run            Do not execute queries
```

If one is running at NERSC, one can load new results into the
`DESC_Twinkles_Level_2` tables by doing
```
[cori04] load_db.py output_repo "Twinkles Run3"
```

The project name, "Twinkles Run3", is used to distinguish these data
from data corresponding to other Level 2 analyses (such as the
"Twinkles Run1.1" results).  This way the `visitId`, `objectId`, and
other table primary keys can be reused. (The `project` column has been
added to the Level 2 [baseline table
schemas](https://lsst-web.ncsa.illinois.edu/schema/index.php?sVer=baseline)
and made part of the primary key for each.)  The default values of the
`database`, `host`, and `port` options have been set for the
`DESC_Twinkles_Level_2` tables at NERSC. The `--dry_run` option can be
used to show what will be run without executing anything.

Note that for Twinkles Run1.1, we only used the `CcdVisit`, `Object`,
and `ForcedSource` tables and the columns of those tables that were
needed for simple light curve analyses.  This script fills only those
columns.
