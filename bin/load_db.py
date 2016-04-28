#!/usr/bin/env python
"""
Script to create and load CcdVisit, Object, and ForcedSource tables.
"""
from __future__ import absolute_import, print_function
import os
import sys
from warnings import filterwarnings
from collections import OrderedDict
import MySQLdb as Database
import desc.pserv
import desc.pserv.utils as pserv_utils
from desc.pserv.registry_tools import find_registry, get_visits

# Suppress warnings from database module.
filterwarnings('ignore', category=Database.Warning)

def create_table(connection, table_name, dry_run=False, clobber=False):
    """
    Create the specified table using the corresponding script in the
    sql subfolder.
    """
    if clobber and not dry_run:
        connection.apply('drop table if exists %s' % table_name)
    create_script = os.path.join(os.environ['PSERV_DIR'], 'sql',
                                 'create_%s.sql' % table_name)
    connection.run_script(create_script, dry_run=dry_run)

def create_tables(connection, tables=('CcdVisit', 'Object', 'ForcedSource'),
                  dry_run=False, clobber=False):
    "Create the CcdVisit, Object, and ForcedSource tables."
    for table_name in tables:
        create_table(connection, table_name, dry_run=dry_run, clobber=clobber)

def ingest_forced_catalogs(connection, repo, dry_run=False):
    "Ingest forced source catalogs into ForcedSource table."
    visits = get_visits(repo)
    failed_ingests = OrderedDict()
    for band, visit_list in visits.items():
        print("Processing band", band, "for", len(visit_list), "visits.")
        sys.stdout.flush()
        for ccdVisitId in visit_list:
            visit_name = 'v%i-f%s' % (ccdVisitId, band)
            #
            # @todo: Generalize this for arbitrary rafts and sensors.
            # This will need the data butler subset method to be fixed
            # first.
            #
            catalog_file = os.path.join(repo, 'forced', '0',
                                        visit_name, 'R22', 'S11.fits')
            print("Processing", visit_name)
            sys.stdout.flush()
            if not dry_run:
                try:
                    pserv_utils.ingest_ForcedSource_data(connection,
                                                         catalog_file,
                                                         ccdVisitId)
                except Exception as eobj:
                    failed_ingests[visit_name] = eobj
    return failed_ingests

if __name__ == '__main__':
    import argparse

    description = """Script to create and load CcdVisit, Object, and Forced
Source tables with Level 2 pipeline ouput."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('repo', help='Repository with Stack output')
    parser.add_argument('--database', type=str, default='DESC_Twinkles_Level_2',
                        help='Database to use')
    parser.add_argument('--mysql_config', type=str, default='~/.my.cnf',
                        help='MySQL config file')
    parser.add_argument('--dry_run', default=False, action='store_true',
                        help='Do not execute queries')
    args = parser.parse_args()

    connect = desc.pserv.DbConnection(db=args.database,
                                      read_default_file=args.mysql_config)

    create_tables(connect, dry_run=args.dry_run)

    registry_file = find_registry(args.repo)
    if args.dry_run:
        print("Ingest registry file", registry_file)
        print("Ingest calexp info")
    else:
        pserv_utils.ingest_registry(connect, registry_file)
        pserv_utils.ingest_calexp_info(connect, args.repo)

    object_catalog = os.path.join(args.repo, 'deepCoadd-results/merged/0/0,0',
                                  'ref-0-0,0.fits')
    if args.dry_run:
        print("Ingest object catalog", object_catalog)
    else:
        pserv_utils.ingest_Object_data(connect, object_catalog)

    failures = ingest_forced_catalogs(connect, args.repo, dry_run=args.dry_run)
    print(failures)
