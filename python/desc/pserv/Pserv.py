"""
Pserv: Practice LSST database server code.
"""
from __future__ import absolute_import, print_function
import copy
import csv
import json
from builtins import zip
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
        except MySQLdb.DatabaseError as eobj:
            cursor.close()
            raise eobj
        cursor.close()
        if cursorFunc is _nullFunc:
            self._mysql_connection.commit()
        return results

    def load_csv(self, table_name, csv_file):
        """
        Load a csv file into the specified table.  Non-char data has
        to be type converted explicitly using a cast for those columns.
        """
        # Get the column names and data types.
        query = """SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_NAME='%(table_name)s'""" % locals()
        data_types = self.apply(query, lambda curs: tuple(x for x in curs))
        query = """LOAD DATA LOCAL INFILE '%(csv_file)s'
                   INTO TABLE %(table_name)s
                   FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
                   IGNORE 1 LINES (""" % locals()
        column_names = tuple(x[0] for x in data_types)
        self.check_column_names(column_names, csv_file)
        query += ',\n'.join(column_names) + ')'
        # Check for conversions from non-char(n) data types.
        conversions = [dt_pair for dt_pair in data_types
                       if dt_pair[1].find('char') == -1]
        dtypes = dict((('int', 'SIGNED'),
                       ('bigint', 'SIGNED'),
                       ('tinyint', 'SIGNED'),
                       ('float', 'DECIMAL(50,25)'),
                       ('double', 'DECIMAL(65,30)')))
        if conversions:
            query += ' set \n'
            cast_list = []
            for column_name, data_type in conversions:
                my_dtype = dtypes[data_type]
                cast_list.append(
                    '%(column_name)s=cast(%(column_name)s as %(my_dtype)s)'
                    % locals())
            query += ',\n'.join(cast_list) + ';'
        self.apply(query)

    @staticmethod
    def check_column_names(column_names, csv_file):
        "Check the column names againts those in the csv file."
        with open(csv_file, 'r') as csv_input:
            csv_cols = csv_input.readline().strip().split(',')
        if len(csv_cols) != len(column_names):
            raise RuntimeError('Number of columns in csv file do not match '
                               + 'the number of columns of db table.')
        for csv_col, table_col in zip(csv_cols, column_names):
            if csv_col != table_col:
                message = 'Column name mismatch between csv file and db table:'
                message += ' %s vs %s' % (csv_col, table_col)
                raise RuntimeError(message)

def create_csv_file_from_fits(fits_file, fits_hdunum, csv_file,
                              column_mapping=None):
    "Create a csv file from a FITS binary table."
    hdulist = fits.open(fits_file)
    bintable = hdulist[fits_hdunum]
    if column_mapping is None:
        column_mapping = dict([(coldef.name, coldef.name)
                               for coldef in bintable.columns])
    with open(csv_file, 'w') as csv_output:
        writer = csv.writer(csv_output, delimiter=',')
        writer.writerow(list(column_mapping.keys()))
        nrows = bintable.header['NAXIS2']
        columns = []
        for csv_name, colname in column_mapping.items():
            if isinstance(colname, basestring):
                coldata = bintable.data[colname].tolist()
                columns.append(coldata)
            else: # Assume colname is a numeric constant.
                columns.append([colname]*nrows)
        data = zip(*tuple(columns))
        for row in data:
            writer.writerow(row)

