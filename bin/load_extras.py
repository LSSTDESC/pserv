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

# Suppress warnings from database module.
filterwarnings('ignore')

def ingest_forced_src_extras(connection, repo_info, project, tract=0,
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
    visits = repo_info.get_visits()
    sensors = repo_info.get_sensors()
    failed_ingests = OrderedDict()
    for band, visit_list in visits.items():
        print("Processing band", band, "for", len(visit_list), "visits.")
        sys.stdout.flush()
        for visitId in visit_list:
            visit_name = 'v%i-f%s' % (visitId, band)
            for raft, sensor in sensors:
                print("Processing", visit_name, 'R'+raft, 'S'+sensor)
                sys.stdout.flush()
                ccdVisitId = pserv_utils.make_ccdVisitId(visitId, raft, sensor)
                column_mapping['ccdVisitId'] = ccdVisitId
                query = 'select zeroPoint from CcdVisit where ccdVisitId=%i' \
                        % ccdVisitId
                zeroPoint =\
                        connection.apply(query,
                                         lambda curs: [x[0] for x in curs][0])
                flux_calibrator = pserv_utils.FluxCalibrator(zeroPoint)
                callbacks = {}
                for value in column_mapping.values():
                    callbacks[value] = flux_calibrator
                    catalog_file = os.path.join(repo, 'forced', str(tract),
                                                visit_name, 'R'+raft[:3:2],
                                                'S'+sensor[:3:2]+'.fits')
                if not dry_run:
                    try:
                        create_csv_file_from_fits(catalog_file, fits_hdunum,
                                                  csv_file,
                                                  column_mapping=column_mapping,
                                                  callbacks=callbacks)
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

    repo_info = desc.pserv.RepositoryInfo(args.repo)

    connect = desc.pserv.DbConnection(database=args.database,
                                      host=args.host,
                                      port=args.port)

    pserv_utils.create_table(connect, 'ForcedSourceExtra',
                             dry_run=args.dry_run, clobber=args.clobber)

    failures = ingest_forced_src_extras(connect, repo_info, args.project,
                                        dry_run=args.dry_run)
    print(failures)
