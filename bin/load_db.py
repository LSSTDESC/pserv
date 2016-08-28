#!/usr/bin/env python
"""
Script to create and load CcdVisit, Object, and ForcedSource tables.
"""
from __future__ import absolute_import, print_function
import os
import sys
from warnings import filterwarnings
from collections import OrderedDict
import desc.pserv
import desc.pserv.utils as pserv_utils
from desc.pserv.registry_tools import find_registry, get_visits

# Suppress warnings from database module.
filterwarnings('ignore')

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

def ingest_forced_catalogs(connection, repo, project, raft='2,2',
                           sensor='1,1', tract=0, dry_run=False):
    """
    Ingest forced source catalogs into ForcedSource table.  The
    CcdVisit table must be filled first so that the zero point flux
    can be retrieved.
    """
    visits = get_visits(repo)
    failed_ingests = OrderedDict()
    for band, visit_list in visits.items():
        print("Processing band", band, "for", len(visit_list), "visits.")
        sys.stdout.flush()
        for visitId in visit_list:
            ccdVisitId = pserv_utils.make_ccdVisitId(visitId, raft, sensor)
            query = 'select zeroPoint from CcdVisit where ccdVisitId=%i' \
                % ccdVisitId
            zeroPoint = connection.apply(query,
                                         lambda curs: [x[0] for x in curs][0])
            def flux_calibration(flux):
                def get_nanomaggies(flux):
                    if flux > 0:
                        return 1e9*flux/zeroPoint
                    else:
                        return np.nan
                try:
                    return np.array([get_nanomaggies(x) for x in flux])
                except TypeError:
                    return get_nanomaggies(flux)
            visit_name = 'v%i-f%s' % (visitId, band)
            catalog_file = os.path.join(repo, 'forced', str(tract),
                                        visit_name, 'R'+raft[:3:2],
                                        'S'+sensor[:3:2]+'.fits')
            print("Processing", visit_name)
            sys.stdout.flush()
            if not dry_run:
                try:
                    pserv_utils.ingest_ForcedSource_data(connection,
                                                         catalog_file,
                                                         ccdVisitId,
                                                         flux_calibration,
                                                         project)
                except Exception as eobj:
                    failed_ingests[visit_name] = eobj
    return failed_ingests

if __name__ == '__main__':
    import argparse

    description = """Script to create and load CcdVisit, Object, and Forced
Source tables with Level 2 pipeline output."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('repo', help='Repository with Stack output')
    parser.add_argument('host', help='Host server for the MySQL database')
    parser.add_argument('project', help='DESC project name')
    parser.add_argument('--database', type=str, default='DESC_Twinkles_Level_2',
                        help='Database to use')
    parser.add_argument('--port', type=str, default='3306',
                        help='Port used by the database host')
    parser.add_argument('--clobber', default=False, action='store_true',
                        help='Drop existing tables and recreate')
    parser.add_argument('--dry_run', default=False, action='store_true',
                        help='Do not execute queries')
    args = parser.parse_args()

    connect = desc.pserv.DbConnection(database=args.database,
                                      host=args.host,
                                      port=args.port)

    create_tables(connect, dry_run=args.dry_run, clobber=args.clobber)

    registry_file = find_registry(args.repo)
    if args.dry_run:
        print("Ingest registry file", registry_file)
        print("Ingest calexp info")
    else:
        pserv_utils.ingest_registry(connect, registry_file, args.project)
        pserv_utils.ingest_calexp_info(connect, args.repo, args.project)

    object_catalog = os.path.join(args.repo, 'deepCoadd-results/merged/0/0,0',
                                  'ref-0-0,0.fits')
    if args.dry_run:
        print("Ingest object catalog", object_catalog)
    else:
        pserv_utils.ingest_Object_data(connect, object_catalog, args.project)

    failures = ingest_forced_catalogs(connect, args.repo, args.project,
                                      dry_run=args.dry_run)
    print(failures)
