"""
Unit tests for pserv package.
"""
from __future__ import absolute_import, print_function
from builtins import zip
import os
from collections import OrderedDict
import csv
from warnings import filterwarnings
import unittest
import numpy as np
import astropy.io.fits as fits
import MySQLdb as Database
import desc.pserv

# Suppress warnings from database module.
filterwarnings('ignore', category=Database.Warning)

def get_db_info():
    """
    Try to connect to Travis CI MySQL services or the user's via
    ~/.my.cnf and return the connection info.  Otherwise, return
    an empty dict, which should skip the tests.
    """
    db_info = {}
    try:
        try:
            # Travis CI usage:
            my_db_info = dict(db='myapp_test', user='travis', host='127.0.0.1')
            test = Database.connect(**my_db_info)
        except Exception, eobj:
            #print(eobj)
            # User's configuration:
            my_db_info = dict(db='test', read_default_file='~/.my.cnf')
            test = Database.connect(**my_db_info)
        test.close()
        db_info = my_db_info
    except Exception, eobj:
        print("No database connection:")
        print(eobj)
    return db_info

_db_info = get_db_info()

@unittest.skipUnless(_db_info, "MySQL database not available")
class PservTestCase(unittest.TestCase):
    def setUp(self):
        """
        Create a connection and test table.
        """
        global _db_info
        self.connection = desc.pserv.LsstDbConnection(**_db_info)
        self.test_table = 'my_test'
        self.fits_file = 'my_test_data.fits'
        self.fits_hdunum = 1
        self.data = (('a', 1, 130.), ('b', 4, 1e-7), ('c', 100, np.pi))
        self._create_test_table()

    def tearDown(self):
        """
        Drop the test table and close the connection.
        """
        self.connection.apply('drop table if exists %s;' % self.test_table)
        del self.connection

    def test_connection_management(self):
        """
        White-box tests of connection pool management.
        """
        global _db_info
        num_cobjs = 3
        connection_objects = [desc.pserv.LsstDbConnection(**_db_info)
                              for i in range(num_cobjs)]
        for cobj in connection_objects:
            key = cobj._conn_key
            # All of the connection objects should be the same since
            # they were built using the same _db_info dict.
            self.assertEqual(self.connection._mysql_connection,
                             cobj._mysql_connection)
            # self.connection is included in the reference count so
            # subtract it from the current count for the comparison.
            self.assertEqual(cobj._LsstDbConnection__connection_refs[key] - 1,
                             len(connection_objects))
            cobj_last = connection_objects.pop()
            del cobj_last

    def _create_fits_bintable(self):
        """
        Create the test FITS file with a binary table with the data in
        self.data.
        """
        hdulist = fits.HDUList()
        hdulist.append(fits.PrimaryHDU())
        colnames = ['KEYWORD', 'INT_VALUE', 'FLOAT_VALUE']
        formats = 'AIE'
        data = list(zip(*self.data))
        columns = [fits.Column(name=colnames[i], format=formats[i],
                               array=data[i]) for i in range(len(colnames))]
        bintable = fits.BinTableHDU.from_columns(columns)
        bintable.name = 'TEST_DATA'
        hdulist.append(bintable)
        if os.path.isfile(self.fits_file):
            os.remove(self.fits_file)
        hdulist.writeto(self.fits_file)

    def _create_test_table(self):
        """
        Create the test table.
        """
        self.connection.apply('drop table if exists %s;' % self.test_table)
        table_name = self.test_table
        query = """create table %s (keywd char(1), int_value int,
                   float_value float);""" % self.test_table
        self.connection.apply(query)

    def _fill_test_table(self):
        """
        Fill the test db table with key/value pairs from self.data.
        """
        table_name = self.test_table
        values = ','.join(["('%s', %i, %e)" % row for row in self.data]) + ';'
        query = "insert into %(table_name)s values %(values)s" % locals()
        self.connection.apply(query)

    def _compare_to_ref_data(self, query_data):
        "Compare data from querys to reference data."
        for query_row, ref_row in zip(query_data, self.data):
            self.assertEqual(query_row[0], ref_row[0])
            self.assertEqual(query_row[1], ref_row[1])
            self.assertAlmostEqual(query_row[2], ref_row[2], places=5)

    def test_apply_cursorFunc(self):
        """
        Test the apply method using a cursor function to retrieve and
        package the query results.
        """
        self._fill_test_table()
        def my_func(curs):
            return dict([row for row in curs])
        query = "select keywd, int_value, float_value from %s" % self.test_table
        table_data = self.connection.apply(
            query, cursorFunc=lambda curs: tuple(x for x in curs))
        self._compare_to_ref_data(table_data)

    def test_create_csv_file_from_fits(self):
        """
        Test the creation of a csv file from a FITS binary table.
        """
        self._create_fits_bintable()
        # column_mapping should be an OrderedDict that maps the columns
        # in the input FITS table to the column name in the db to be
        # loaded with the csv data.
        column_mapping = OrderedDict((('keywd', 'KEYWORD'),
                                      ('int_value', 'INT_VALUE'),
                                      ('float_value', 'FLOAT_VALUE')))
        csv_file = 'test_file.csv'
        desc.pserv.create_csv_file_from_fits(self.fits_file,
                                             self.fits_hdunum,
                                             csv_file,
                                             column_mapping=column_mapping)
        csv_data = []
        with open(csv_file, 'r') as csv_input:
            reader = csv.reader(csv_input, delimiter=',')
            for i, row in enumerate(reader):
                if i == 0:
                    colnames = row
                    continue
                csv_data.append((row[0], int(row[1]), float(row[2])))
        self._compare_to_ref_data(csv_data)
        os.remove(self.fits_file)
        os.remove(csv_file)

#    def test_load_csv(self):
#        """
#        Test that the csv-loaded file returns consistent data from the
#        db table when queried.
#        """
#        column_mapping = OrderedDict(keywd='KEYWORD', value='VALUE')
#        self._create_fits_bintable()
#        csv_file = 'test_file.csv'
#        desc.pserv.create_csv_file_from_fits(self.fits_file,
#                                             self.fits_hdunum,
#                                             csv_file,
#                                             column_mapping=column_mapping)
#        self.connection.load_csv(self.test_table, csv_file)
#        def my_func(curs):
#            "Retrieve key/value pairs as a dict."
#            return dict([row for row in curs])
#        query = "select keywd, value from %s" % self.test_table
#        table_data = self.connection.apply(
#            query, cursorFunc=lambda curs: OrderedDict([row for row in curs]) )
#        self.assertEqual(table_data, self.data)
#        os.remove(self.fits_file)
#        os.remove(csv_file)
#
#    def test_incorrect_cvs_mapping(self):
#        """
#        Test that the column_mapping is checked against the db table
#        when loaded and that an incorrect mapping (wrong column order
#        and incorrect column names) raises an exception.
#        """
#        column_mapping = OrderedDict(keyword='KEYWORD', value='VALUE')
#        desc.pserv.create_cvs_from_fits(self.fits_file, self.fits_ext,
#                                        csvfile, column_mapping=column_mapping)
#        self.assertRaises(RuntimeError, desc.pserv.load_csv,
#                          (self.test_table, csvfile))
#

if __name__ == '__main__':
    unittest.main()
