"""
Ingesting data
"""
from __future__ import (absolute_import,
                        print_function,
                        division,)
import numpy as np
import lsst.daf.persistence as dp
from sqlalchemy import create_engine

class ExposureData(object):
    def __init__(self, repoPath, outputRoot):
        self.bp = dp.Butler(repoPath, outputRoot=outputRoot)

    @staticmethod
    def dataID(visit, tract, band, raft, sensor):
        """
        Parameters
        ----------
        visit : int, mandatory
            integer indexing the visit
        tract : int, mandatory
            integer indexing the tract
        raft : string, mandatory
            coordinates of camera raft in which the CCD is located as a comma
            separated pair
        sensor : string, mandatory
            coordinates of sensor on the CCD in a comma separated pair

        Examples
        --------
        >>> dataID(tract=0, visit=201057, filter='u', raft='2,2', sensor='1,1')

        """
        return dict(tract=tract, visit=visit, filter=band, sensor=sensor, raft=raft)

    def getVisitBackground(self, dataID):
        """
        """
        exps = self.bp.get('calexpBackground', dataID, immediate=True)
        image = exps.getImage().getArray()
        return image

    def getZeroPts(self, dataID):
        """
        return the zero point and the uncertainty on the zero point as a tuple

        Parameters
        ----------
        dataID : dictionary of 

        Returns
        -------
        tuple of (zero point, uncertainty on zero point)
        """
        calexps = self.bp.get('calexp', dataID, immediate=True)
        zp = calexps.getCalib().getFluxMag0()
        zptypes = ('FLOAT', 'FLOAT')
        zpnames = ('zp', 'zperr')
        return zp , zptypes, zpnames


    def getSkyBG(self, dataID, pixelSize=0.2):
        """
        dataID

        pixelSize : optional, float, defaults to LSST value 0.2
        """

        _pixelSize = pixelSize #.2 # arcsec
        bgImage = self.getVisitBackground(dataID)
        skyBG = np.median(bgImage)
        skyNoise = np.std(bgImage)
        zp = self.getZeroPts(dataID)[0][0]
        skybgmag = -2.5 * np.log10(skyBG / _pixelSize / _pixelSize / zp)
        skybginmagsperarcsec2 = skybgmag 
        skybgtypes = ('FLOAT', 'FLOAT', 'FLOAT')
        skybgnames = ('skybackground', 'skybackgroundnoise', 'skybginmagsperarcsec2') 

        return (skyBG, skyNoise, skybginmagsperarcsec2), skybgtypes,  skybgnames


    def dbRecord(self, dataID):
        """
        """
        zp = zip(*self.getZeroPts(dataID))
        skybg = zip(*self.getSkyBG(dataID))

        record = (dataID['visit'], 
                  dataID['raft'],
                  dataID['tract'],
                  dataID['sensor'],
                  dataID['filter'],
                  zp,)
                  # zperr,
                  # skyBG,
                  # skyNoise)
        return record


if __name__ == '__main__':

    engine = create_engine('sqlite:///' + 'forced_test.db')
    repoPath = '/Users/rbiswas/doc/projects/DESC/Twinkles_example_data/Twinkles_example_data/repo'
    outputRoot = '/Users/rbiswas/doc/projects/DESC/Twinkles_example_data/Twinkles_example_data/out'
    expData = ExposureData(repoPath, outputRoot)
    dataID = ExposureData.dataID(tract=0, visit=201057, band='u', raft='2,2',
                                 sensor='1,1')
    # print(expData.getSkyBG(dataID))
    print(expData.dbRecord(dataID))
    exit()
    bp = dp.Butler(repoPath, outputRoot=outputRoot)
    dataID = ExposureData.dataID(tract=0, visit=201057, band='u', raft='2,2',
                                 sensor='1,1')

    print(dataID)

    calexps = bp.get('calexp', dataID, immediate=True)
    zp = calexps.getCalib().getFluxMag0()
    zptypes = ('FLOAT', 'FLOAT')
    zpnames = ('zp', 'zperr')
    print(zp , zptypes, zpnames)
