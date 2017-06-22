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
import lsst.afw.fits
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

    Attributes
    ----------
    zeroPoint : float
        Zero point in ADU for the exposure in question.
    """
    def __init__(self, zeroPoint):
        """
        Class constructor

        Parameters
        ----------
        zeroPoint : float
            Zero point in ADU for the exposure in question.
        """

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
        return 1e9*flux/self.zeroPoint
#        if flux > 0:
#            return 1e9*flux/self.zeroPoint
#        else:
#            return np.nan

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
            return np.array([self.get_nanomaggies(x) for x in flux])
        except TypeError:
            return self.get_nanomaggies(flux)

def make_ccdVisitId(visit, raft, sensor):
    """
    Create the ccdVisitId to be used in the CcdVisit table.

    Parameters
    ----------
    visit : int
         visitId of the desired visit.
    raft : str
         Identifier of the raft location in the LSST focal plane,
         e.g., 'R22'.
    sensor : str
         Identifier of the sensor location in a raft, e.g., 'S11'.

    Returns
    -------
    int
        An integer that uniquely identifies the visit-raft-sensor
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

    Parameters
    ----------
    connection : desc.pserv.DbConnection
        The connection object to use to create the table.
    table_name : str
        The table name corresponding to the script in the sql subfolder.
    dry_run : bool, optional
        If True, just print the table creation code to the screen, but
        don't execute.  The default value is False.
    clobber : bool, optional
        Overwrite the table if it already exists. Default: False
    """
    if clobber and not dry_run:
        connection.apply('drop table if exists %s' % table_name)
    create_script = os.path.join(lsstUtils.getPackageDir('pserv'), 'sql',
                                 'create_%s.sql' % table_name)
    connection.run_script(create_script, dry_run=dry_run)

def get_projectId(connection, projectName, table='Project'):
    """
    Get the ID of the named project from the db table.

    Parameters
    ----------
    connection : desc.pserv.DbConnection
        The connection object to use to modify the CcdVisit table.
    registry_file : str
        The sqlite registry file containing the visit information.
    projectName : str
        The name of the desired project.  This is used to
        differentiate different projects in the MySQL tables that may
        have colliding primary keys, e.g., various runs of Twinkles,
        or PhoSim Deep results.
    """
    query = "select projectId from %s where projectName='%s'" \
            % (table, projectName)
    return connection.apply(query, lambda curs: [x[0] for x in curs])[0]


def ingest_registry(connection, registry_file, projectName):
    """
    Ingest some relevant data from a registry.sqlite3 file into
    the CcdVisit table.

    Parameters
    ----------
    connection : desc.pserv.DbConnection
        The connection object to use to modify the CcdVisit table.
    registry_file : str
        The sqlite registry file containing the visit information.
    projectName : str
        The name of the desired project.  This is used to
        differentiate different projects in the MySQL tables that may
        have colliding primary keys, e.g., various runs of Twinkles,
        or PhoSim Deep results.
    """
    # Set the timezone for the MySQL session to GMT to avoid DST problem
    # ("1292 Incorrect datetime value").
    connection.apply("set time_zone = '+00:00'")

    projectId = get_projectId(connection, projectName)
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
                   obsStart='%(taiObs)s', projectId='%(projectId)s'
                   on duplicate key update
                   visitId=%(visit)i, ccdName='%(ccd)s',
                   raftName='%(raft)s', filterName='%(filter_)s',
                   obsStart='%(taiObs)s'""" % locals()
        try:
            connection.apply(query)
        except Exception as eobj:
            print("query:", query)
            raise eobj

def ingest_calexp_info(connection, repo, projectName):
    """Extract information such as zeroPoint, seeing, sky background, sky
    noise, etc., from the calexp products and insert the values into
    the CcdVisit table.

    Parameters
    ----------
    connection : desc.pserv.DbConnection
        The connection object to use to modify the CcdVisit table.
    repo : str
        The path the output data repository used by the Stack.
    projectName : str
        The name of the desired project.  This is used to
        differentiate different projects in the MySQL tables that may
        have colliding primary keys, e.g., various runs of Twinkles,
        or PhoSim Deep results.
    """
    projectId = get_projectId(connection, projectName)
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
        try:
            calexp = dataref.get('calexp')
            calexp_bg = dataref.get('calexpBackground')
        except lsst.afw.fits.FitsError as eobj:
            print("FitsError:", str(eobj))
            continue
        ccdVisitId = make_ccdVisitId(dataref.dataId['visit'],
                                     dataref.dataId['raft'],
                                     dataref.dataId['sensor'])

        # Compute zeroPoint, seeing, skyBg, skyNoise column values.
        try:
            zeroPoint = calexp.getCalib().getFluxMag0()[0]
        except:
            continue
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
                   projectId='%(projectId)s'""" % locals()
        connection.apply(query)
        nrows += 1
    print('!')

def ingest_ForcedSource_data(connection, catalog_file, ccdVisitId,
                             flux_calibration, projectName,
                             psFlux='base_PsfFlux_flux',
                             psFlux_Sigma='base_PsfFlux_fluxSigma',
                             flags=0, fits_hdunum=1, csv_file='temp.csv',
                             cleanup=True):
    """
    Load the forced source catalog data into the ForcedSource table.
    Create a temporary csv file to take advantage of the efficient
    'LOAD DATA LOCAL INFILE' facility.
    Parameters
    ----------
    connection : desc.pserv.DbConnection
        The connection object to use to modify the CcdVisit table.
    catalog_file : str
        The path to the catalog file produced by the forcedPhotCcd.py
        task.
    ccdVisitId : int
        Unique identifier of the visit-raft-sensor combination.
    flux_calibration : function
        A callback function to convert from ADU to nanomaggies. Usually,
        this is a desc.pserv.FluxCalibrator object.
    projectName : str
        The name of the desired project.  This is used to
        differentiate different projects in the MySQL tables that may
        have colliding primary keys, e.g., various runs of Twinkles,
        or PhoSim Deep results.
    psFlux : str, optional
        The column name from the forced source catalog to use for the
        point source flux in the ForcedSource table.
        Default: 'base_PsfFlux_flux'
    psFlux_Sigma : str, optional
        The column name from the forced source catalog to use for the
        uncertainty in the point source flux in the ForcedSource table.
        Default: 'base_PsfFlux_fluxSigma'
    flags : int, optional
        Value to insert in the flag column in the ForcedSource table.
        This is not really used.  Default: 0
    fits_hdunum : int, optional
        The HDU number of the binary table containing the forced source
        data.
    csv_file : str, optional
        The file name to use for the csv file written to use with the
        'LOAD DATA LOCAL INFILE' statement. Default: 'temp.csv'
    cleanup : bool, optional
        Flag to delete the csv_file after loading the data. Default: True
    """
    projectId = get_projectId(connection, projectName)
    column_mapping = OrderedDict((('objectId', 'objectId'),
                                  ('ccdVisitId', ccdVisitId),
                                  ('psFlux', psFlux),
                                  ('psFlux_Sigma', psFlux_Sigma),
                                  ('flags', flags),
                                  ('projectId', projectId)))
    # Callbacks to apply calibration and convert to nanomaggies.
    callbacks = dict(((psFlux, flux_calibration),
                      (psFlux_Sigma, flux_calibration)))
    create_csv_file_from_fits(catalog_file, fits_hdunum, csv_file,
                              column_mapping=column_mapping,
                              callbacks=callbacks)
    connection.load_csv('ForcedSource', csv_file)
    if cleanup:
        os.remove(csv_file)

def ingest_Object_data(connection, catalog_file, projectName):
    """
    Ingest the reference catalog from the merged coadds.

    Parameters
    ----------
    connection : desc.pserv.DbConnection
        The connection object to use to modify the Object table.
    catalog_file : str
        The path to the file of the merged coadd catalog file produced
        by Level 2 analysis.
    projectName : str
        The name of the desired project.  This is used to
        differentiate different projects in the MySQL tables that may
        have colliding primary keys, e.g., various runs of Twinkles,
        or PhoSim Deep results.
    """
    projectId = get_projectId(connection, projectName)
    data = fits.open(catalog_file)[1].data
    nobjs = len(data['id'])
    print("Ingesting %i objects" % nobjs)
    sys.stdout.flush()
    nrows = 0
    for objectId, ra, dec, parent, extendedness \
            in zip(data['id'],
                   data['coord_ra'],
                   data['coord_dec'],
                   data['parent'],
                   data['base_ClassificationExtendedness_value']):
        if nrows % int(nobjs/20) == 0:
            sys.stdout.write('.')
            sys.stdout.flush()
        ra_val = ra*180./np.pi
        dec_val = dec*180./np.pi
        if np.isnan(extendedness):
            extendedness = 1.
        query = """insert into Object
                   (objectId, parentObjectId, psRa, psDecl, extendedness,
                   projectId)
                   values (%i, %i, %17.9e, %17.9e, %17.9e, '%s')
                   on duplicate key update psRa=%17.9e, psDecl=%17.9e,
                   extendedness=%17.9e""" \
            % (objectId, parent, ra_val, dec_val, extendedness, projectId,
               ra_val, dec_val, extendedness)
        connection.apply(query)
        nrows += 1
    print("!")
