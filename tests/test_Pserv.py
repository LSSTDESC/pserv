"""
Unit tests for pserv package.
"""
import unittest
from warnings import filterwarnings
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
            #print eobj
            # User's configuration:
            my_db_info = dict(db='test', read_default_file='~/.my.cnf')
            test = Database.connect(**my_db_info)
        test.close()
        db_info = my_db_info
    except Exception, eobj:
        print "No database connection:"
        print eobj
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
        self.connection.apply('drop table if exists %s;' % self.test_table)
        self.create_test_table()

    def tearDown(self):
        """
        Drop the test table and close the connection.
        """
        self.connection.apply('drop table if exists %s;' % self.test_table)
        del self.connection

    def create_test_table(self):
        """
        Create and fill test table with key/value pairs.
        """
        table_name = self.test_table
        self.data = dict(a=1, b=3, c=4, d=0)
        query = 'create table %s (keywd char(1), value int);' % self.test_table
        self.connection.apply(query)
        values = ','.join(["('%s', %i)" % kv for kv in self.data.items()]) + ';'
        query = "insert into %(table_name)s values %(values)s" % locals()
        self.connection.apply(query)

    def test_apply_cursorFunc(self):
        """
        Test the apply method using a cursor function to retrieve and
        package the query results.
        """
        def my_func(curs):
            "Retrieve key/value pairs as a dict."
            return dict([row for row in curs])
        query = "select keywd, value from %s" % self.test_table
        table_data = self.connection.apply(
            query, cursorFunc=lambda curs: dict([row for row in curs]) )
        self.assertEqual(self.data, table_data)

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

    def test_csv_load(self):
        pass

if __name__ == '__main__':
    unittest.main()
