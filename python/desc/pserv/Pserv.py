"""
Pserv: Practice LSST database server code.
"""
from __future__ import absolute_import, print_function
from builtins import zip
from collections import OrderedDict
import copy
import csv
import json
import astropy.io.fits as fits
import MySQLdb

__all__ = ['LsstDbConnection', 'create_csv_file_from_fits']

def _nullFunc(*args):
    """
    Default do-nothing function for processing data from a MySQLdb
    cursor object.
    """
    return None

class LsstDbConnection(object):
    """
    Class to manage MySQL connections using Borg attribute management.
    """
    __connection_pool = dict()
    __connection_refs = dict()
    def __init__(self, **kwds):
        """
        Constructor to make the connection object.
        """
        kwds['local_infile'] = 1  # enable LOAD LOCAL INFILE
        self._get_mysql_connection(kwds)

    def __del__(self):
        """
        Decrement reference counts to the connection object and close
        it if ref counts is zero.
        """
        self.__connection_refs[self._conn_key] -= 1
        if self.__connection_refs[self._conn_key] == 0:
            self._mysql_connection.close()
            del self.__connection_pool[self._conn_key]
            del self.__connection_refs[self._conn_key]

    def _get_mysql_connection(self, kwds_par):
        """
        Update the connection pool and reference counts, and set the
        self._mysql_connection reference.
        """
        kwds = copy.deepcopy(kwds_par)
        try:
            del kwds['table_name']
        except KeyError:
            pass
        # Serialize the kwds dict to obtain a hashable key for the
        # self.__connection_pool and self.__connection_refs dicts.
        self._conn_key = json.dumps(kwds, sort_keys=True)

        if not self.__connection_pool.has_key(self._conn_key):
            # Create a new mysql connection object.
            self.__connection_pool[self._conn_key] = MySQLdb.connect(**kwds)

        # Update the reference counts for the connection objects.
        try:
            self.__connection_refs[self._conn_key] += 1
        except KeyError:
            self.__connection_refs[self._conn_key] = 1

        self._mysql_connection = self.__connection_pool[self._conn_key]

    def apply(self, query, cursorFunc=_nullFunc):
        """
        Apply the query, optionally using the cursorFunc to process
        the query results.
        """
        cursor = self._mysql_connection.cursor()
        try:
            cursor.execute(query)
            results = cursorFunc(cursor)
        except MySQLdb.DatabaseError, message:
            cursor.close()
            raise MySQLdb.DatabaseError(message)
        cursor.close()
        if cursorFunc is _nullFunc:
            self._mysql_connection.commit()
        return results

    def load_csv(self, table_name, csv_file):
        """
        Load a csv file into the specified table.
        """
        # Get the column names and data types.
        query = """SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_NAME='%(table_name)s'""" % locals()
        data_types = self.apply(query,
                                lambda curs: OrderedDict(x for x in curs))
        print(data_types)
        query = """LOAD DATA LOCAL INFILE '%(csv_file)s'
                   INTO TABLE %(table_name)s
                   FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
                   IGNORE 1 LINES
                   (keywd, value) set value=cast(value as decimal(30,30));""" % locals()
        self.apply(query)

def create_csv_file_from_fits(fits_file, fits_hdunum, csv_file,
                              column_mapping=None):
    hdulist = fits.open(fits_file)
    bintable = hdulist[fits_hdunum]
    if column_mapping is None:
        column_mapping = dict([(coldef.name, coldef.name)
                               for coldef in bintable.columns])
    with open(csv_file, 'w') as csv_output:
        writer = csv.writer(csv_output, delimiter=',')
        writer.writerow(list(column_mapping.keys()))
        data = zip(*tuple(bintable.data[colname].tolist()
                          for colname in column_mapping.values()))
        for row in data:
            writer.writerow(row)

