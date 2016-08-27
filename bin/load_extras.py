#!/usr/bin/env python
"""
Script to create and load the ForcedSourceExtra table.
"""
from __future__ import absolute_import, print_function
import os
import sys
from warnings import filterwarnings
from collections import OrderedDict
import desc.pserv
import desc.pserv.utils as pserv_utils
from desc.pserv.registry_tools import find_registry, get_visits
from .load_db import create_tables

# Suppress warnings from database module.
filterwarnings('ignore')

def ingest_forced_src_extras(connection, repo, project, raft, sensor,
                             fits_hdunum=1, csv_file='temp.csv',
                             dry_run=True):
    column_mapping =\
        OrderedDict(objectId='objectId',
                    ccdVisitId=None,
                    ap_3_0_Flux='base_CircularApertureFlux_3_0_flux',
                    ap_3_0_Flux_Sigma='base_CircularApertureFlux_3_0_fluxSigma',
                    ap_9_0_Flux='base_CircularApertureFlux_9_0_flux',
                    ap_9_0_Flux_Sigma='base_CircularApertureFlux_9_0_fluxSigma',
                    ap_25_0_Flux='base_CircularApertureFlux_25_0_flux',
                    ap_25_0_Flux_Sigma='base_CircularApertureFlux_25_0_fluxSigma',
                    psf_apCorr_Flux='base_psfFlux_apCorr_flux',
                    psf_apCorr_Flux_Sigma='base_psfFlux_apCorr_fluxSigma',
                    gauss_apCorr_Flux='gaussFlux_apCorr_flux',
                    gauss_apCorr_Flux_Sigma='gaussFlux_apCorr_fluxSigma',
                    flags=0, project=project)
    visits = get_visits(repo)
    failed_ingests = OrderedDict()
    for band, visit_list in visits.items():
        for visitId in visit_list:
            ccdVisitId = pserv_utils.make_ccdVisitId(visitId, raft, sensor)
            column_mapping['ccdVisitId'] = ccdVisitId
            query = 'select zeroPoint from CcdVisit where ccdVisitId=%i' \
                    % ccdVisitId
            zeroPoint = connection.apply(query,
                                         lambda curs: [x[0] for x in curs][0])
            scale_factors = {}
            for value in column_mappings.values():
                scale_factors[value] = 1e9/zeroPoint
            visit_name = 'v%i-f%s' % (visitId, band)
            print("Processing", visit_name)
            sys.stdout.flush()
            #
            # @todo: Generalize this for tract values other than '0'.
            #
            catalog_file = os.path.join(repo, 'forced', '0',
                                        visit_name, 'R'+raft[:3:2],
                                        'S'+sensor[:3:2]+'.fits')
            if not dry_run:
                try:
                    create_csv_file_from_fits(catalog_file, fits_hdunum,
                                              csv_file,
                                              column_mapping=column_mapping,
                                              scale_factors=scale_factors)
                    connection.load_csv('ForceSourceExtras', csv_file)
                    try:
                        os.remove(csv_file)
                    except OSerror:
                        pass
                except Exception as eobj:
                    failed_ingests[visit_name] = eobj
    return failed_ingests

if __name__ == '__main__':
    import argparse

    description = """Script to create and load ForcedSourceExtra table with Level 2 pipeline output."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('repo', help='Repository with Stack output')
    parser.add_argument('project', help='DESC project name')
    parser.add_argument('raft', type=str, default='2,2', help='Raft id')
    parser.add_argument('sensor', type=str, default='1,1', help='Sensor id')
    parser.add_argument('--database', type=str, default='DESC_Twinkles_Level_2',
                        help='Database to use')
    parser.add_argument('--host', type=str, default='scidb1.nersc.gov',
                        help='Host server for the MySQL database')
    parser.add_argument('--port', type=str, default='3306',
                        help='Port used by the database host')
    parser.add_argument('--clobber', default=False, action='store_true',
                        help='Drop existing table and recreate')
    parser.add_argument('--dry_run', default=False, action='store_true',
                        help='Do not execute queries')
    args = parser.parse_args()

    connect = desc.pserv.DbConnection(database=args.database,
                                      host=args.host,
                                      port=args.port)

    create_tables(connect, tables=('ForcedSourceExtra',),
                  dry_run=args.dry_run, clobber=args.clobber)

    failures = ingest_forced_src_extras(connect, args.repo, args.project,
                                        args.raft, args.sensor,
                                        dry_run=args.dry_run)
    print(failures)
