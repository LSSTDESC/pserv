"""
Utilities for ingesting Stack data products into the MySQL tables.
"""
from __future__ import absolute_import, print_function, division
import os
import sys
from collections import OrderedDict
import sqlite3
import numpy as np
import astropy.io.fits as fits
import lsst.afw.math as afwMath
import lsst.daf.persistence as dp
import lsst.utils as lsstUtils
from .Pserv import create_csv_file_from_fits

__all__ = ['FluxCalibrator', 'make_ccdVisitId', 'create_table',
           'ingest_registry', 'ingest_calexp_info',
           'ingest_ForcedSource_data', 'ingest_Object_data']

class FluxCalibrator(object):
    """
    Functor class to convert uncalibrated fluxes from a given exposure
    to nanomaggies.

    Parameters
    ----------
    zeroPoint : float
        Zero point in ADU for the exposure in question.

    Attributes
    ----------
    zeroPoint : float
        Zero point in ADU for the exposure in question.

    """
    def __init__(self, zeroPoint):
        "Class constructor"
        self.zeroPoint = zeroPoint

    def get_nanomaggies(self, flux):
        """
        Convert flux to nanomaggies.

        Parameters
        ----------
        flux : float
            Measure source flux in ADU.

        Returns
        -------
        float
            Source flux in nanomaggies.
        """
        if flux > 0:
            return 1e9*flux/self.zeroPoint
        else:
            return np.nan

    def __call__(self, flux):
        """
        Convert flux to nanomaggies.

        Parameters
        ----------
        flux : float or sequence
            Measure source flux(es) in ADU.

        Returns
        -------
        float or np.array
            Source flux(es) in nanomaggies.
        """
        try:
            return np.array([self.get_nanomaggies(flux) for x in flux])
        except TypeError:
            return self.get_nanomaggies(flux)

def make_ccdVisitId(visit, raft, sensor):
    """
    Return an integer that uniquely identifies a visit-raft-sensor
    combination.
    """
    # There are around 2.5 million visits in the 10 year survey, so 7
    # digits should suffice for the visit part.  Prepend the RRSS
    # combination to that and return as an int.
    ccdVisitId = int(raft[:3:2] + sensor[:3:2] + "%07i" % visit)
    return ccdVisitId

def create_table(connection, table_name, dry_run=False, clobber=False):
    """
    Create the specified table using the corresponding script in the
    sql subfolder.
    """
    if clobber and not dry_run:
        connection.apply('drop table if exists %s' % table_name)
    create_script = os.path.join(lsstUtils.getPackageDir('pserv'), 'sql',
                                 'create_%s.sql' % table_name)
    connection.run_script(create_script, dry_run=dry_run)

def ingest_registry(connection, registry_file, project):
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
        ccdVisitId = make_ccdVisitId(visit, raft, ccd)
        query = """insert into CcdVisit set ccdVisitId=%(ccdVisitId)i,
                   visitId=%(visit)i, ccdName='%(ccd)s',
                   raftName='%(raft)s', filterName='%(filter_)s',
                   obsStart='%(taiObs)s', project='%(project)s'
                   on duplicate key update
                   visitId=%(visit)i, ccdName='%(ccd)s',
                   raftName='%(raft)s', filterName='%(filter_)s',
                   obsStart='%(taiObs)s'""" % locals()
        try:
            connection.apply(query)
        except Exception as eobj:
            print("query:", query)
            raise eobj

def ingest_calexp_info(connection, repo, project):
    """
    Extract information such as zeroPoint, seeing, sky background, sky
    noise, etc., from the calexp products and insert the values into
    the CcdVisit table.
    """
    # Use the Butler to find all of the visit/sensor combinations.
    butler = dp.Butler(repo)
    datarefs = butler.subset('calexp')
    num_datarefs = len(datarefs)
    print('Ingesting %i visit/sensor combinations' % num_datarefs)
    sys.stdout.flush()
    nrows = 0
    for dataref in datarefs:
        if nrows % int(num_datarefs/20) == 0:
            sys.stdout.write('.')
            sys.stdout.flush()
        calexp = dataref.get('calexp')
        calexp_bg = dataref.get('calexpBackground')
        ccdVisitId = make_ccdVisitId(dataref.dataId['visit'],
                                     dataref.dataId['raft'],
                                     dataref.dataId['sensor'])

        # Compute zeroPoint, seeing, skyBg, skyNoise column values.
        zeroPoint = calexp.getCalib().getFluxMag0()[0]
        # For the psf_fwhm (=seeing) calculation, see
        # https://github.com/lsst/meas_deblender/blob/master/python/lsst/meas/deblender/deblend.py#L227
        pixel_scale = calexp.getWcs().pixelScale().asArcseconds()
        seeing = (calexp.getPsf().computeShape().getDeterminantRadius()
                  *2.35*pixel_scale)
        # Retrieving the nominal background image is computationally
        # expensive and just returns an interpolated version of the
        # stats_image (see
        # https://github.com/lsst/afw/blob/master/src/math/BackgroundMI.cc#L87),
        # so just get the stats image.
        #bg_image = calexp_bg.getImage()
        bg_image = calexp_bg[0][0].getStatsImage()
        skyBg = afwMath.makeStatistics(bg_image, afwMath.MEDIAN).getValue()
        skyNoise = afwMath.makeStatistics(calexp.getMaskedImage(),
                                          afwMath.STDEVCLIP).getValue()
        query = """update CcdVisit set zeroPoint=%(zeroPoint)15.9e,
                   seeing=%(seeing)15.9e,
                   skyBg=%(skyBg)15.9e, skyNoise=%(skyNoise)15.9e
                   where ccdVisitId=%(ccdVisitId)i and
                   project='%(project)s'""" % locals()
        connection.apply(query)
        nrows += 1
    print('!')

def ingest_ForcedSource_data(connection, catalog_file, ccdVisitId,
                             flux_calibration, project,
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
                                  ('flags', flags),
                                  ('project', project)))
    # Callbacks to apply calibration and convert to nanomaggies.
    callbacks = dict(((psFlux, flux_calibration),
                      (psFlux_Sigma, flux_calibration)))
    create_csv_file_from_fits(catalog_file, fits_hdunum, csv_file,
                              column_mapping=column_mapping,
                              callbacks=callbacks)
    connection.load_csv('ForcedSource', csv_file)
    if cleanup:
        os.remove(csv_file)

def ingest_Object_data(connection, catalog_file, project):
    "Ingest the reference catalog from the merged coadds."
    data = fits.open(catalog_file)[1].data
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
                   (objectId, parentObjectId, psRa, psDecl, project)
                   values (%i, %i, %17.9e, %17.9e, '%s')
                   on duplicate key update psRa=%17.9e, psDecl=%17.9e""" \
            % (objectId, parent, ra_val, dec_val, project, ra_val, dec_val)
        connection.apply(query)
        nrows += 1
    print("!")

