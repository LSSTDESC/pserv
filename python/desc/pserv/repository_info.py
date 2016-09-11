'''
Tools for extracting information about an LSST Stack output
respository.
'''
from __future__ import absolute_import, print_function
import os
import pickle
import itertools
from collections import OrderedDict
import sqlite3
import astropy.time

__all__ = ['RepositoryInfo']

class RepositoryInfo(object):
    """
    Class for extracting information about an LSST Stack output
    respository.

    Parameters
    ----------
    repo : str
        Path (relative or absolute) to the output respository created
        by the Stack.
    registry_name : str, optional
        Filename of the sqlite registry file.

    Attributes
    ----------
    repo : str
        Full path to the output respository created by the Stack.
    registry_file : str
        Full path to the registry file.

    """
    def __init__(self, repo, registry_name='registry.sqlite3'):
        "Class constructor"
        self.repo = repo
        self.registry_file = self.find_registry(repo, registry_name)

    @staticmethod
    def find_registry(repo, registry_name):
        """
        Find the sqlite registry file given the output repository path.

        Parameters
        ----------
        repo : str
            Path (relative or absolute) to the output respository created
            by the Stack.
        registry_name : str
            Filename of the sqlite registry file.

        Returns
        -------
        str
            Absolute path to the registry file.
        """
        basePath = os.path.abspath(repo)
        while not os.path.exists(os.path.join(basePath, registry_name)):
            if os.path.exists(os.path.join(basePath, "_parent")):
                basePath = os.path.join(basePath, "_parent")
            else:
                raise RuntimeError("Could not find registry file")
        return os.path.join(basePath, registry_name)

    def get_visit_mjds(self):
        """
        Get a dictionary of visit MJDs, keyed by visit number.

        Returns
        -------
        OrderedDict
            A dictionary of visit MJDs, keyed by visit number.
        """
        conn = sqlite3.connect(self.registry_file)
        mjds = OrderedDict()
        query = """select visit, taiObs from raw where channel='0,0'
                   order by visit asc"""
        for row in conn.execute(query):
            visit_time = astropy.time.Time(row[1], format='isot')
            mjds[row[0]] = visit_time.mjd
        return mjds

    def get_visits(self):
        """
        Get a dictionary of visits ids, keyed by 'ugrizy' filter.

        Returns
        -------
        OrderedDict
            A dictionary of visits ids, keyed by 'ugrizy' filter.
        """
        conn = sqlite3.connect(self.registry_file)
        filters = 'ugrizy'
        visits = OrderedDict([(filter_, []) for filter_ in filters])
        for filter_ in filters:
            query = "select visit from raw_visit where filter='%s'" % filter_
            for row in conn.execute(query):
                visits[filter_].append(row[0])
        return visits

    def get_sensors(self):
        """
        Get the list of sensors, each identified by raftID-sensorID tuple,
        e.g., [ ('2,2', '0,0'), ('2,2', '0,1'), ... ('2,2', '2,2')].

        Returns
        -------
        list
            A list of raftID-sensorID tuples.
        """
        conn = sqlite3.connect(self.registry_file)
        query = "select distinct raft, ccd from raw"
        sensors = []
        for row in conn.execute(query):
            sensors.append(tuple(row))
        return sensors

    def get_patches(self):
        """
        Get the tracts and patches from the <repo>/deepCoadd/skyMap.pickle
        file.

        Returns
        -------
        dict
            A dictionary of lists of patches, keyed by tract.
        """
        deepCoadd_dir = os.path.abspath(os.path.join(self.repo, 'deepCoadd'))
        with open(os.path.join(deepCoadd_dir, 'skyMap.pickle')) as f:
            skymap = pickle.load(f)
        patches = {}
        for tract_info in skymap:
            nx, ny = [tract_info.getNumPatches()[i] for i in (0, 1)]
            patches[tract_info.getId()] = \
                list('%i,%i' % x for x in itertools.product(list(range(nx)),
                                                            list(range(ny))))
        return patches
