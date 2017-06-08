"""
Unit tests for pserv package.
"""
from __future__ import absolute_import, print_function
import os
import csv
import unittest
from collections import OrderedDict
from warnings import filterwarnings
import ConfigParser
import numpy as np
import astropy.io.fits as fits
import desc.pserv

filterwarnings('ignore')

def get_db_info():
    """
    Try to connect to Travis CI MySQL services or the user's via
    ~/.my.cnf and return the connection info.  Otherwise, return
    an empty dict, which should skip the tests.
    """
    try:
        try:
            # Travis CI usage:
            db_info = dict(database='myapp_test', username='travis',
                           host='127.0.0.1', port='3306')
            test = desc.pserv.DbConnection(**db_info)
        except RuntimeError as eobj:
            print(eobj)
            # Read the user's default configuration from ~/.my.cnf
            parser = ConfigParser.ConfigParser()
            parser.read(os.path.join(os.environ['HOME'], '.my.cnf'))
            db_info = dict(parser.items('client'))
            db_info['database'] = 'test'
            if db_info.has_key('user'):
                del db_info['user']
            if db_info.has_key('password'):
                del db_info['password']
            test = desc.pserv.DbConnection(**db_info)
    except Exception as eobj:
        print("No database connection:")
        print(eobj)
        db_info = {}
    return db_info

_db_info = get_db_info()

@unittest.skipUnless(_db_info, "MySQL database not available")
class PservTestCase(unittest.TestCase):
    """
    TestCase for Pserv module.
    """
    def setUp(self):
        """
        Create a connection and test table.
        """
        self.connection = desc.pserv.DbConnection(**_db_info)
        self.test_table = 'my_test'
        self.project = 'my project'
        self.data = (('a', 1, 130., 3.1943029977e-24, self.project),
                     ('b', 4, 1.4938229e-20, 4.408099891e10, self.project),
                     ('c', 100, np.pi, np.pi, self.project),
                     ('d', 3, 4.9039542e20, 9.487982348e30, self.project))
        self._create_test_table()
        # FITS/csv related set up:
        self.fits_file = 'my_test_data.fits'
        self._create_fits_bintable()
        self.csv_file = self._create_csv_file()

    def tearDown(self):
        """
        Drop the test table and close the connection.
        """
        self.connection.apply('drop table if exists %s;' % self.test_table)
        del self.connection
        # FITS/csv related tear down:
        if os.path.isfile(self.fits_file):
            os.remove(self.fits_file)
        if os.path.isfile(self.csv_file):
            os.remove(self.csv_file)

    def _create_test_table(self):
        """
        Create the test table.
        """
        self.connection.apply('drop table if exists %s;' % self.test_table)
        query = """create table %s (keywd char(1), int_value int,
                   float_value float, double_value double,
                   project char(30));""" % self.test_table
        self.connection.apply(query)

    def _fill_test_table(self):
        """
        Fill the test db table with key/value pairs from self.data.
        """
        table_name = self.test_table
        values = ','.join(["('%s', %i, %e, %e, '%s')" % row
                           for row in self.data]) + ';'
        query = "insert into %(table_name)s values %(values)s" % locals()
        self.connection.apply(query)

    def _query_test_table(self):
        """
        Query for the test table contents.
        """
        query = """select keywd, int_value, float_value, double_value,
                   project from %s""" % self.test_table
        return self.connection.apply(
            query, cursorFunc=lambda curs: tuple(x for x in curs))

    def _compare_to_ref_data(self, query_data, places=5):
        "Compare data from querys to reference data."
        for query_row, ref_row in zip(query_data, self.data):
            self.assertEqual(query_row[0], ref_row[0])
            self.assertEqual(query_row[1], ref_row[1])
            format_ = '%.' + str(places) + 'e'
            fp1 = format_ % query_row[2]
            fp2 = format_ % ref_row[2]
            self.assertEqual(fp1, fp2)
            self.assertEqual(query_row[4], self.project)
            #self.assertAlmostEqual(query_row[2], ref_row[2], places=places)

    def test_apply_cursorFunc(self):
        """
        Test the apply method using a cursor function to retrieve and
        package the query results.
        """
        self._fill_test_table()
        table_data = self._query_test_table()
        self._compare_to_ref_data(table_data)

    def _create_fits_bintable(self):
        """
        Create the test FITS file with a binary table with the data in
        self.data.
        """
        hdulist = fits.HDUList()
        hdulist.append(fits.PrimaryHDU())
        colnames = ['KEYWORD', 'INT_VALUE', 'FLOAT_VALUE', 'DOUBLE_VALUE']
        formats = 'AIED'
        data = list(zip(*self.data))
        columns = [fits.Column(name=colnames[i], format=formats[i],
                               array=data[i]) for i in range(len(colnames))]
        bintable = fits.BinTableHDU.from_columns(columns)
        bintable.name = 'TEST_DATA'
        hdulist.append(bintable)
        if os.path.isfile(self.fits_file):
            os.remove(self.fits_file)
        hdulist.writeto(self.fits_file)

    def _create_csv_file(self, csv_file='test_file.csv',
                         column_mapping=None, fits_hdnum=1):
        """
        Create a csv file from the FITS binary table.
        """
        if column_mapping is None:
            column_mapping = OrderedDict((('keywd', 'KEYWORD'),
                                          ('int_value', 'INT_VALUE'),
                                          ('float_value', 'FLOAT_VALUE'),
                                          ('double_value', 'DOUBLE_VALUE'),
                                          ('project', self.project)))
        desc.pserv.create_csv_file_from_fits(self.fits_file, fits_hdnum,
                                             csv_file,
                                             column_mapping=column_mapping)
        return csv_file

    @staticmethod
    def _read_csv_file(csv_file):
        csv_data = []
        with open(csv_file, 'r') as csv_input:
            reader = csv.reader(csv_input, delimiter=',')
            for i, row in enumerate(reader):
                if i == 0:
                    # Skip the header line.
                    continue
                csv_data.append((row[0], int(row[1]), float(row[2]),
                                 np.float64(row[3]), row[4]))
        return csv_data

    def test_create_csv_file_from_fits(self):
        """
        Test the creation of a csv file from a FITS binary table.
        """
        csv_data = self._read_csv_file(self.csv_file)
        self._compare_to_ref_data(csv_data)

    def test_create_csv_file_from_fits_with_constant_columns(self):
        """
        Test the creation of csv file from a FITS binary table with
        constant numeric column values set in the column_mapping.
        """
        int_value = 52
        column_mapping = OrderedDict((('keywd', 'KEYWORD'),
                                      ('int_value', int_value),
                                      ('float_value', 'FLOAT_VALUE'),
                                      ('double_value', 'DOUBLE_VALUE'),
                                      ('project', self.project)))
        csv_file = self._create_csv_file(column_mapping=column_mapping)
        csv_data = self._read_csv_file(csv_file)
        for csv_row, ref_row in zip(csv_data, self.data):
            self.assertEqual(csv_row[0], ref_row[0])
            self.assertEqual(csv_row[1], int_value)
            fp1 = '%.5e' % csv_row[2]
            fp2 = '%.5e' % ref_row[2]
            self.assertEqual(fp1, fp2)
            fp1 = '%.10e' % csv_row[3]
            fp2 = '%.10e' % ref_row[3]
            self.assertEqual(fp1, fp2)

        float_value = 719.3449
        column_mapping = OrderedDict((('keywd', 'KEYWORD'),
                                      ('int_value', 'INT_VALUE'),
                                      ('float_value', float_value),
                                      ('double_value', 'DOUBLE_VALUE'),
                                      ('project', self.project)))
        csv_file = self._create_csv_file(column_mapping=column_mapping)
        csv_data = self._read_csv_file(csv_file)
        for csv_row, ref_row in zip(csv_data, self.data):
            self.assertEqual(csv_row[0], ref_row[0])
            self.assertEqual(csv_row[1], ref_row[1])
            fp1 = '%.5e' % csv_row[2]
            fp2 = '%.5e' % float_value
            self.assertEqual(fp1, fp2)
            fp1 = '%.10e' % csv_row[3]
            fp2 = '%.10e' % ref_row[3]
            self.assertEqual(fp1, fp2)

    def test_create_csv_file_from_fits_with_callbacks(self):
        """
        Test the creation of a csv file from a FITS binary table with
        callback functions applied (e.g., conversion to nanomaggies
        using the zero point flux) applied to certain columns.
        """
        column_mapping = OrderedDict((('keywd', 'KEYWORD'),
                                      ('int_value', 'INT_VALUE'),
                                      ('float_value', 'FLOAT_VALUE'),
                                      ('double_value', 'DOUBLE_VALUE'),
                                      ('project', self.project)))
        callbacks = dict((('FLOAT_VALUE', lambda x: 2.981*x),
                          ('DOUBLE_VALUE', lambda x: 0.321*x)))
        csv_file = 'test_file_scaling.csv'
        fits_hdunum = 1
        desc.pserv.create_csv_file_from_fits(self.fits_file, fits_hdunum,
                                             csv_file,
                                             column_mapping=column_mapping,
                                             callbacks=callbacks)
        csv_data = self._read_csv_file(csv_file)
        for csv_row, ref_row in zip(csv_data, self.data):
            self.assertEqual(csv_row[0], ref_row[0])
            self.assertEqual(csv_row[1], ref_row[1])
            fp1 = '%.5e' % csv_row[2]
            fp2 = '%.5e' % (callbacks['FLOAT_VALUE'](ref_row[2]))
            self.assertEqual(fp1, fp2)
            fp1 = '%.10e' % csv_row[3]
            fp2 = '%.10e' % (callbacks['DOUBLE_VALUE'](ref_row[3]))
            self.assertEqual(fp1, fp2)
        os.remove(csv_file)

    def test_load_csv(self):
        """
        Test that after loading the csv file generated from FITS data,
        a query returns data consistent with the reference data.
        """
        csv_file = self.csv_file
        self.connection.load_csv(self.test_table, csv_file)
        table_data = self._query_test_table()
        self._compare_to_ref_data(table_data)

    def test_incorrect_csv_mapping(self):
        """
        Test that an incorrect column mapping raises a RuntimeError.
        """
        # Test incorrect column ordering.
        column_mapping = OrderedDict((('keywd', 'KEYWORD'),
                                      ('float_value', 'FLOAT_VALUE'),
                                      ('int_value', 'INT_VALUE'),
                                      ('double_value', 'DOUBLE_VALUE')))
        csv_file = self._create_csv_file(column_mapping=column_mapping)
        self.assertRaises(RuntimeError, self.connection.load_csv,
                          *(self.test_table, csv_file))

        # Test incorrect column name.
        column_mapping = OrderedDict((('keyword', 'KEYWORD'),
                                      ('int_value', 'INT_VALUE'),
                                      ('float_value', 'FLOAT_VALUE'),
                                      ('double_value', 'DOUBLE_VALUE')))
        csv_file = self._create_csv_file(column_mapping=column_mapping)
        self.assertRaises(RuntimeError, self.connection.load_csv,
                          *(self.test_table, csv_file))

        # Test incorrect number of columns.
        column_mapping = OrderedDict((('keyword', 'KEYWORD'),
                                      ('int_value', 'INT_VALUE'),
                                      ('float_value', 'FLOAT_VALUE'),
                                      ('float2_value', 'FLOAT_VALUE'),
                                      ('double_value', 'DOUBLE_VALUE')))
        csv_file = self._create_csv_file(column_mapping=column_mapping)
        self.assertRaises(RuntimeError, self.connection.load_csv,
                          *(self.test_table, csv_file))
        os.remove(csv_file)

    def test_create_schema_from_fits(self):
        "Test the creation of a schema from a FITS binary table."
        catalog_file = os.path.join(os.environ['PSERV_DIR'], 'tests',
                                    'ref-0-10,11_truncated.fits.gz')
        sql_file = 'bintable_schema.sql'
        fits_hdunum = 1
        table_name = 'deepCoadd_catalog'
        desc.pserv.create_schema_from_fits(catalog_file, fits_hdunum, sql_file,
                                           table_name,
                                           primary_key='id, project',
                                           add_columns=('project INT',))
        with open(sql_file) as schema:
            lines = [x.strip() for x in schema.readlines()]
        self.assertIn('id BIGINT,', lines)
        self.assertIn('coord_ra DOUBLE,' , lines)
        self.assertIn('deblend_nChild INT,' , lines)
        self.assertIn('base_SdssShape_xxSigma FLOAT,' , lines)
        self.assertIn('FLAGS1 BIGINT,', lines)
        self.assertIn('FLAGS2 BIGINT,', lines)
        self.assertIn('primary key (id, project)', lines)
        self.assertIn('project INT,', lines)

if __name__ == '__main__':
    unittest.main()
