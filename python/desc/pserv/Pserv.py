"""
Pserv: Practice LSST database server code.
"""
from __future__ import absolute_import, print_function
import copy
import csv
import json
from collections import OrderedDict
import numpy as np
import astropy.io.fits as fits
import sqlalchemy
import lsst.daf.persistence as dp

__all__ = ['DbConnection', 'create_csv_file_from_fits',
           'create_schema_from_fits', 'pack_flags']

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

        Parameters
        ----------
        **kwds : **dict
            keyword arguments with the database info.  Minimally,
            this would include host and port, but can also include
            the database name.
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

        Parameters
        ----------
        kwds_par : dict
            Dictionary of connection info to pass to sqlalchemy.
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

    def apply(self, sql, cursorFunc=_nullFunc):
        """
        Apply an SQL statement, optionally using the cursorFunc to
        process any query results.

        Parameters
        ----------
        sql : str
            An SQL statement.

        cursorFunc : callback function, optional
            Functor used to process the output of an SQL statement
            For non-queries, a do-nothing null object is passed by
            default.

        """
        cursor = self._mysql_connection.cursor()
        cursor.execute(sql)
        results = cursorFunc(cursor)
        cursor.close()
        if cursorFunc is _nullFunc:
            self._mysql_connection.commit()
        return results

    def run_script(self, script, dry_run=False):
        """Execute a script of SQL code.

        Parameters
        ----------
        script : str
            Name of the file containing the code.

        dry_run : bool, optional
            If True, just print the SQL code to the screen, but don't
            execute.  The default value is False.
        """
        with open(script) as script_data:
            sql = ''.join(script_data.readlines())
        if dry_run:
            print(sql)
        else:
            self.apply(sql)

    def load_csv(self, table_name, csv_file):
        """
        Load a csv file into the specified table.

        Parameters
        ----------
        table_name : str
            The name of the db table to load into.

        csv_file : str
            The name of the csv file containing the data.

        Notes
        -----
        Non-char data has to be type converted explicitly using a cast
        for those columns.
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
        sql = """LOAD DATA LOCAL INFILE '%(csv_file)s'
                 INTO TABLE %(table_name)s
                 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
                 IGNORE 1 LINES (""" % locals()
        column_names = tuple(x[0] for x in data_types)
        self.check_column_names(column_names, csv_file)
        sql += ',\n'.join(column_names) + ')'
        # Check for conversions from non-char(n) data types.
        conversions = [dt_pair for dt_pair in data_types
                       if dt_pair[1].find('char') == -1]
        dtypes = dict((('int', 'SIGNED'),
                       ('bigint', 'SIGNED'),
                       ('tinyint', 'SIGNED'),
                       ('float', 'DECIMAL(50,25)'),
                       ('double', 'DECIMAL(65,30)')))
        if conversions:
            sql += ' set \n'
            cast_list = []
            for column_name, data_type in conversions:
                my_dtype = dtypes[data_type]
                cast_list.append(
                    '%(column_name)s=cast(%(column_name)s as %(my_dtype)s)'
                    % locals())
            sql += ',\n'.join(cast_list) + ';'
        self.apply(sql)

    @staticmethod
    def check_column_names(column_names, csv_file):
        """
        Check the column names against those in the csv file.

        Parameters
        ----------
        column_names : sequence
            The column names expected to be in the csv file
        csv_file : str
            The name of the csv file containing the data.

        Raises
        ------
        RuntimeError
            If there is any mismatch between the expected columns and
            the ones in the csv file.
        """
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
    """
    Create a csv file from a FITS binary table.

    Parameters
    ----------
    fits_file : str
         Name of the FITS file.
    fits_hdunum : int
         HDU number of the binary table to process.
    csv_file : str
         Name of the csv file to create.
    column_mapping : dict, optional
         Mapping between column names in the FITS file and the column
         names to be used in the csv file.  The latter should match
         the columns in the db table to be filled.  By default, the
         FITS table column names are used.
    callbacks : dict, optional
         A dictionary of optional callback functions to apply to a
         column, keyed by FITS table column name.  This is used to
         apply any simple transformations to the column, e.g., scaling
         by flux zeropoint or units conversion.
    """
    if callbacks is None:
        callbacks = {}
    nbits = 64
    bintable = fits.open(fits_file)[fits_hdunum]
    if column_mapping is None:
        column_mapping = OrderedDict()
        for coldef in bintable.columns:
            if coldef.format[-1] == 'X':
                num_fields = int(np.ceil(float(coldef.format[:-1])/nbits))
                for ifield in range(num_fields):
                    name = '%s%i' % (coldef.name.upper(), ifield + 1)
                    column_mapping[name] = name
            else:
                column_mapping[coldef.name] = coldef.name
    with open(csv_file, 'w') as csv_output:
        writer = csv.writer(csv_output, delimiter=',', lineterminator='\n')
        colnames = list(column_mapping.keys())
        writer.writerow(colnames)
        nrows = bintable.header['NAXIS2']
        columns = []
        bintable_colnames = [x.name for x in bintable.columns]
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

def create_schema_from_fits(fits_file, hdunum, outfile, table_name,
                            primary_key='', add_columns=()):
    """
    Create an SQL schema from a FITS binary table.

    Parameters
    ----------
    fits_file : str
        The filename of the FITS file with the binary table.
    hdunum : int
        The HDU number of the binary table.
    outfile : str
        The filename of the output file to contain the SQL schema.
    table_name : str
        The name of the table to create.
    primary_key : str, optional
        The primary key to use.  Default: ''.
    add_columns : tuple, optional
        Columns to add to the schema that are not in the FITS table, e.g.,
        add_columns=('project INT',).  Default: ().
    """
    padding = 7*' '
    bin_table = fits.open(fits_file)[hdunum]
    with open(outfile, 'w') as output:
        output.write('create table if not exists %s (\n' % table_name)
        for column in bin_table.columns:
            write_schema_column(output, column, padding)
        for column in add_columns:
            output.write('%s%s,\n' % (padding, column))
        output.write('%sprimary key (%s)\n' % (padding, primary_key) +
                     '%s)\n' % padding)

def write_schema_column(output, column, padding):
    """
    Write a schema column given a column description from astropy.io.fits.

    Parameters
    ----------
    output : file
        The file object to write the schema column entry.
    column : astropy.io.fits.column.Column
        The table column for which to write the SQL schema column.
    padding : str
        The padding string to prepend to the SQL schema column line.
    """
    type_map = {'1D' : 'DOUBLE',
                '1E' : 'FLOAT',
                '1K' : 'BIGINT',
                '1J' : 'INT',
                '1I' : 'SMALLINT'}
    format = column.format.strip("'")
    if format[-1] == 'X':
        write_bit_schema_column(output, column, padding)
        return
    if int(format[:-1]) != 1:
        # Skip vector columns.
        return
    output.write('%s%s %s,\n' % (padding, column.name, type_map[format]))

def write_bit_schema_column(output, column, padding):
    """
    Write schema columns as BIGINT types to contain FITS bit columns
    of format 'NNNX', e.g, a FITS column with format '142X' will produce
    3 (= ceil(142./64)) SQL table columns.  Following the Qserv baseline
    schema convention for the "FLAGS" columns, the column.name will be
    converted to upper case and the column number (starting with '1')
    will be appended, e.g., name='flags', format='142X' will produce
    'FLAGS1 BIGINT,', 'FLAGS2 BIGINT,', 'FLAGS3 BIGINT,' SQL lines.

    Parameters
    ----------
    output : file
        The file object to write the schema column entry.
    column : astropy.io.fits.column.Column
        The table column for which to write the SQL schema column.
    padding : str
        The padding string to prepend to the SQL schema column line.
    """
    mysql_type = 'BIGINT'
    colsize = 64
    format = column.format.strip("'")
    num_bits = float(format[:-1])
    ncols = int(np.ceil(num_bits/colsize))
    for icol in range(ncols):
        name = '%s%i' % (column.name.upper(), icol + 1)
        output.write('%s%s %s,\n' % (padding, name, mysql_type))

def pack_flags(flags, nbits=64):
    """
    Pack an array of boolean flags into integers with nbits bits.

    Parameters
    ----------
    flags : np.array
        numpy array of bools.
    nbits : int, optional
        Number of bits per integer.  Default: 64.

    Returns
    -------
    list : A list of integers with the packed flags.
    """
    num_ints = int(np.ceil(float(len(flags))/nbits))
    subarrs = [flags[i*nbits:(i+1)*nbits] for i in range(num_ints)]
    values = [sum([long(2**i) for i, flag in enumerate(subarr) if flag])
              for subarr in subarrs]
    return values
