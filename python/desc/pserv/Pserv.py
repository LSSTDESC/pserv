"""
Pserv: Practice LSST database server code.
"""
import copy
import json
import MySQLdb


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
