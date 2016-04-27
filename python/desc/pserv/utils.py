"""
Utilities for ingesting Stack data products into the MySQL tables.
"""
from __future__ import absolute_import, print_function, division
import os
import sys
from collections import OrderedDict
import numpy as np
import sqlite3
import MySQLdb as Database
import astropy.io.fits as fits
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.daf.persistence as dp
from .Pserv import DbConnection, create_csv_file_from_fits

def ingest_registry(connection, registry_file):
    """
    Ingest some relevant data from a registry.sqlite3 file into
    the CcdVisit table.
    """
    registry = sqlite3.connect(registry_file)
    query = """select taiObs, visit, filter, raft, ccd,
               expTime from raw where channel='0,0' order by visit asc"""
    for row in registry.execute(query):
        taiObs, visit, filter_, raft, ccd, expTime = tuple(row)
        taiObs = taiObs[:len('2016-03-18 00:00:00.000000')]
        query = """insert into CcdVisit set ccdVisitId=%(visit)i,
                   visitId=%(visit)i, ccdName='%(ccd)s',
                   raftName='%(raft)s', filterName='%(filter_)s',
                   obsStart='%(taiObs)s'
                   on duplicate key update
                   visitId=%(visit)i, ccdName='%(ccd)s',
                   raftName='%(raft)s', filterName='%(filter_)s',
                   obsStart='%(taiObs)s'""" % locals()
        try:
            connection.apply(query)
        except Database.DatabaseError as eobj:
            print("query:", query)
            raise eobj

def ingest_calexp_info(connection, repo):
    """
    Extract information such as seeing, sky background, sky noise,
    etc., from the calexp results and insert the values into the
    CcdVisit table.
    """
    # Use the Butler to find all of the visit/sensor combinations.
    butler = dp.Butler(repo)
    datarefs = butler.subset('calexp')
    for dataref in datarefs:
        calexp = dataref.get('calexp')
        calexp_bg = dataref.get('calexpBackground')

        # @todo Need to update this to use a value that takes into account
        # the raft and sensor info.
        ccdVisitId = dataref.dataId['visit']

        # Compute seeing, skyBg, skyNoise column values.
        pixel_scale = calexp.getWcs().pixelScale().asArcseconds()
        # For psf_fwhm (=seeing) calculation, see
        # https://github.com/lsst/meas_deblender/blob/master/python/lsst/meas/deblender/deblend.py#L227
        seeing = (calexp.getPsf().computeShape().getDeterminantRadius()
                  *2.35*pixel_scale)
        # Retrieving the nominal background image is computationally
        # expensive and just returns an interpolated version of the
        # stats_image (see
        # https://github.com/lsst/afw/blob/master/src/math/BackgroundMI.cc#L87).
        # So get the stats image instead.
        #bg_image = calexp_bg.getImage()
        bg_image = calexp_bg[0][0].getStatsImage()
        skyBg = afwMath.makeStatistics(bg_image, afwMath.MEDIAN).getValue()
        skyNoise = afwMath.makeStatistics(calexp.getMaskedImage(),
                                          afwMath.VARIANCECLIP).getValue()
        query = """update CcdVisit set seeing=%(seeing)e,
                   skyBg=%(skyBg)e, skyNoise=%(skyNoise)e
                   where ccdVisitId=%(ccdVisitId)i""" % locals()
        connection.apply(query)

def ingest_ForcedSource_data(connection, catalog_file, ccdVisitId,
                             psFlux='base_PsfFlux_flux',
                             psFlux_Sigma='base_PsfFlux_fluxSigma',
                             flags=0, fits_hdunum=1, csv_file='temp.csv',
                             cleanup=True):
    """
    Load the forced source catalog data into the ForcedSource table.
    Create a temporary csv file to take advantage of the efficient
    'LOAD DATA LOCAL INFILE' facility.
    """
    column_mapping = OrderedDict((('objectId', 'objectId'),
                                  ('ccdVisitId', ccdVisitId),
                                  ('psFlux', psFlux),
                                  ('psFlux_Sigma', psFlux_Sigma),
                                  ('flags', flags)))
    create_csv_file_from_fits(catalog_file, fits_hdunum, csv_file,
                              column_mapping=column_mapping)
    connection.load_csv('ForcedSource', csv_file)
    if cleanup:
        os.remove(csv_file)

def ingest_Object_data(connection, catalog_file):
    "Ingest the reference catalog from the merged coadds."
    hdulist = fits.open(catalog_file)
    data = hdulist[1].data
    nobjs = len(data['id'])
    print("Ingesting %i objects" % nobjs)
    sys.stdout.flush()
    nrows = 0
    for objectId, ra, dec, parent in zip(data['id'],
                                         data['coord_ra'],
                                         data['coord_dec'],
                                         data['parent']):
        if nrows % int(nobjs/20) == 0:
            sys.stdout.write('.')
            sys.stdout.flush()
        ra_val = ra*180./np.pi
        dec_val = dec*180./np.pi
        query = """insert into Object
                   (objectId, parentObjectId, psRa, psDecl)
                   values (%i, %i, %17.9e, %17.9e)
                   on duplicate key update psRa=%17.9e, psDecl=%17.9e""" \
            % (objectId, parent, ra_val, dec_val, ra_val, dec_val)
        connection.apply(query)
        nrows += 1
    print("!")
