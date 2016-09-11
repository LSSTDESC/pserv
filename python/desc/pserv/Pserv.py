"""
Pserv: Practice LSST database server code.
"""
from __future__ import absolute_import, print_function
import copy
import csv
import json
from collections import OrderedDict
import astropy.io.fits as fits
import sqlalchemy
import lsst.daf.persistence as dp

__all__ = ['DbConnection', 'create_csv_file_from_fits']

def _nullFunc(*args):
    """
    Default do-nothing function for processing data from a DBAPI 2
    cursor object.
    """
    return None

class DbConnection(object):
    """
    Class to manage db connections using sqlalchemy and DbAuth.
    """
    def __init__(self, **kwds):
        """
        Constructor to make the connection object.
        """
        if not kwds.has_key('query'): # enable LOAD LOCAL INFILE
            kwds['query'] = dict()
        kwds['query']['local_infile'] = 1
        # Use lsst.daf.persistence.DbAuth to get username and password
        # from ~/.lsst/db-auth.paf
        kwds['username'] = dp.DbAuth.username(kwds['host'], str(kwds['port']))
        kwds['password'] = dp.DbAuth.password(kwds['host'], str(kwds['port']))

        self._get_mysql_connection(kwds)

    def _get_mysql_connection(self, kwds_par):
        """
        Set the self._mysql_connection attribute
        """
        kwds = copy.deepcopy(kwds_par)
        try:
            del kwds['table_name']
        except KeyError:
            pass
        try:
            # Always use the 'mysql+mysqldb' driver so remove any
            # user-specified driver.
            del kwds['driver']
        except KeyError:
            pass

        # Create a new mysql connection object.
        db_url = sqlalchemy.engine.url.URL('mysql+mysqldb', **kwds)
        engine = sqlalchemy.create_engine(db_url)
        self._mysql_connection = engine.raw_connection()

    def apply(self, query, cursorFunc=_nullFunc):
        """
        Apply the query, optionally using the cursorFunc to process
        the query results.
        """
        cursor = self._mysql_connection.cursor()
        cursor.execute(query)
        results = cursorFunc(cursor)
        cursor.close()
        if cursorFunc is _nullFunc:
            self._mysql_connection.commit()
        return results

    def run_script(self, script, dry_run=False):
        "Execute a script of sql."
        with open(script) as script_data:
            query = ''.join(script_data.readlines())
        if dry_run:
            print(query)
        else:
            self.apply(query)

    def load_csv(self, table_name, csv_file):
        """
        Load a csv file into the specified table.  Non-char data has
        to be type converted explicitly using a cast for those columns.
        """
        # Get the column names and data types.
        query = """SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE TABLE_NAME='%(table_name)s'""" % locals()
        def dtype_tuple(curs):
            dtypes = OrderedDict()
            for x in curs:
                dtypes[x] = 1
            return tuple(dtypes.keys())
        data_types = self.apply(query, cursorFunc=dtype_tuple)
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
        "Check the column names against those in the csv file."
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
                              column_mapping=None, callbacks=None):
    "Create a csv file from a FITS binary table."
    bintable = fits.open(fits_file)[fits_hdunum]
    if column_mapping is None:
        column_mapping = OrderedDict([(coldef.name, coldef.name)
                                      for coldef in bintable.columns])
    if callbacks is None:
        callbacks = {}
    with open(csv_file, 'w') as csv_output:
        writer = csv.writer(csv_output, delimiter=',', lineterminator='\n')
        colnames = list(column_mapping.keys())
        writer.writerow(colnames)
        nrows = bintable.header['NAXIS2']
        columns = []
        bintable_colnames = [x.name for x in bintable.columns.columns]
        for colname in column_mapping.values():
            if colname in bintable_colnames:
                coldata = bintable.data[colname]
                try:
                    coldata = callbacks[colname](coldata)
                except KeyError:
                    pass
                columns.append(coldata.tolist())
            else: # Assume colname is a numeric or string constant.
                columns.append([colname]*nrows)
        for row in zip(*tuple(columns)):
            writer.writerow(row)
