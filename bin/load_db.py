#!/usr/bin/env python
"""
Script to create and load CcdVisit, Object, and ForcedSource tables.
"""
from __future__ import absolute_import, print_function, division
import os
import sys
from warnings import filterwarnings
from collections import OrderedDict
import numpy as np
import desc.pserv
import desc.pserv.utils as pserv_utils

# Suppress warnings from database module.
filterwarnings('ignore')

def create_tables(connection, tables=('CcdVisit', 'Object', 'ForcedSource'),
                  dry_run=False, clobber=False):
    "Create the CcdVisit, Object, and ForcedSource tables."
    for table_name in tables:
        pserv_utils.create_table(connection, table_name, dry_run=dry_run,
                                 clobber=clobber)

def ingest_forced_catalogs(connection, repo_info, project, tract=0,
                           dry_run=False):
    """
    Ingest forced source catalogs into ForcedSource table.  The
    CcdVisit table must be filled first so that the zero point flux
    can be retrieved.
    """
    visits = repo_info.get_visits()
    sensors = repo_info.get_sensors()
    failed_ingests = OrderedDict()
    for band, visit_list in visits.items():
        print("Processing band", band, "for", len(visit_list), "visits.")
        sys.stdout.flush()
        for visitId in visit_list:
            for raft, sensor in sensors:
                ccdVisitId = pserv_utils.make_ccdVisitId(visitId, raft, sensor)
                query = 'select zeroPoint from CcdVisit where ccdVisitId=%i' \
                        % ccdVisitId
                zeroPoint = connection.apply(query,
                                             lambda c: [x[0] for x in c][0])
                flux_calibrator = pserv_utils.FluxCalibrator(zeroPoint)
                visit_name = 'v%i-f%s' % (visitId, band)
                catalog_file = os.path.join(repo, 'forced', str(tract),
                                            visit_name, 'R'+raft[:3:2],
                                            'S'+sensor[:3:2]+'.fits')
                print("Processing", visit_name, 'R'+raft, 'S'+sensor)
                sys.stdout.flush()
                if not dry_run:
                    try:
                        pserv_utils.ingest_ForcedSource_data(connection,
                                                             catalog_file,
                                                             ccdVisitId,
                                                             flux_calibrator,
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
    parser.add_argument('project', help='DESC project name')
    parser.add_argument('--database', type=str, default='DESC_Twinkles_Level_2',
                        help='Database to use')
    parser.add_argument('--host', type=str, default='scidb1.nersc.gov',
                        help='Host server for the MySQL database')
    parser.add_argument('--port', type=str, default='3306',
                        help='Port used by the database host')
    parser.add_argument('--clobber', default=False, action='store_true',
                        help='Drop existing tables and recreate')
    parser.add_argument('--dry_run', default=False, action='store_true',
                        help='Do not execute queries')
    args = parser.parse_args()

    repo_info = desc.pserv.RepositoryInfo(args.repo)

    connect = desc.pserv.DbConnection(database=args.database,
                                      host=args.host,
                                      port=args.port)

    create_tables(connect, dry_run=args.dry_run, clobber=args.clobber)

    if args.dry_run:
        print("Ingest registry file", repo_info.registry_file)
        print("Ingest calexp info")
    else:
        pserv_utils.ingest_registry(connect, repo_info.registry_file,
                                    args.project)
        pserv_utils.ingest_calexp_info(connect, args.repo, args.project)

    patches = repo_info.get_patches()
    for tract, patch_list in patches.items():
        for patch in patch_list:
            object_catalog \
                = os.path.join(args.repo, 'deepCoadd-results/merged', tract,
                               patch, 'ref-%(tract)s-%(patch)s.fits' % locals())
            if args.dry_run:
                print("Ingest object catalog", object_catalog)
            else:
                pserv_utils.ingest_Object_data(connect, object_catalog,
                                               args.project)

    failures = ingest_forced_catalogs(connect, repo_info, args.project,
                                      dry_run=args.dry_run)
    print(failures)
