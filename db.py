#!/usr/bin/env python3

from enum import Enum
import traceback

import mysql.connector
from mysql.connector import errorcode
from mysql.connector import errors

from config import LOG, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE


class Queries:
    # Simple query to find an IP address using a client_id key
    IP = (
        'SELECT ip '
        'FROM client '
        'WHERE cli.clientid=? '
        )
    INSERT_CLIENTLOG = (
        'INSERT INTO log SET clientid=?, mac=?, dt=now(), type=?, '
        'result=?, ip=?, msg=?, stage=?, fw=?, extcfgtype=?, product=?, '
        'hw=?, protocolversion=1'
        )

class DbUtils():

    @staticmethod
    def utf_decode(bytes_in):
        try:
            return bytes_in.decode('utf-8')
        except AttributeError:
            return bytes_in

    @staticmethod
    def load_data(db_data, gaps_keys_mapping, index=0):
        """
            Loads database query result into a dictionary to be used
            during the session
                Params:
                    db_data: Database query result in tuple format
                    gaps_keys_mapping: Mapping of each element in the database
                    query result to a corresponding gaps_key, in the correct
                    positioning (list)
                Returns:
                    Dictionary where each key is a RPCS key mapped to a value
                    retrieved from the database according to given mapping
        """

        loaded_data = {}

        for gaps_key in gaps_keys_mapping:
            if isinstance(gaps_key, dict):
                # Recurse in order to preserve mapping structure
                for client_data, gaps_keys_mapping in gaps_key.items():
                    loaded_data[client_data] = DbUtils.load_data(db_data, gaps_keys_mapping, index)
            else:
                try:
                    loaded_data[gaps_key] = DbUtils.utf_decode(db_data[index])
                    index += 1
                except IndexError:
                    # No [more] data to load
                    break

        return loaded_data


class DbException(Exception, Enum):
    """
        Defines the exceptions that can be raised by the class Db
    """
    CONNECTION_FAILED = 'Cannot connect to database'
    QUERY_NO_RESULT = 'Query does not return any results'
    MYSQL_CONNECTOR = 'Problem with the mysql connector module'
    NO_VALID_PARAMETER = 'No valid parameter specified'
    INVALID_MAC_ADDRESS = 'Non-matching MAC-address'

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        # Get the name of the function that raised this error
        function = traceback.extract_stack(None, 2)[0][2]
        return 'Database error: {} in function {}'.format(self.msg, function)


class Db:
    """
        Database functions
        This module shall not be imported as a whole, but as:

        from db import *
        from db import QUERY_x, QUERY_x

        Import only the queries that are needed
    =====
        The queries are prepared in the cursor the first time they are used
        to have faster execution times when the query is used again

        Todo: Can multiple queries be used in a prepared cursor?
        Or do we need multiple cursors?
    """

    def __init__(self):
        """
         Instantiate the class
         No input parameters
        """
        self.__cnx = None
        self.__cursor = None

    def connect(self):
        """
         Connect to the database and create a cursor
         No input parameters
        """
        config = {
            'user': MYSQL_USER,
            'password': MYSQL_PASSWORD,
            'database': MYSQL_DATABASE,
            'unix_socket': '/run/mysqld/mysqld.sock',
            'raise_on_warnings': False,
            'charset': 'latin1',
            'collation': 'latin1_general_ci',
            'buffered': False,
            }

        try:
            self.__cnx = mysql.connector.connect(**config)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                LOG.error("Cannot connect to database: No access")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                LOG.error("Cannot connect to database: Unknown database")
            else:
                LOG.error("Cannot connect to database: Unknown error")

            raise DbException.CONNECTION_FAILED

        self.__cursor = self.__cnx.cursor(prepared=True)

    def disconnect(self):
        """
         Disconnect from database
         Input parameter: cnx = connection
        """
        if self.__cnx is None:
            LOG.error("Error: disconnecting when not connected")
            return
        try:
            # Try to clean up database cursor object. If that fails the garbage
            # collector will automatically deal with it after connection has
            # been closed.
            self.__cursor.close()
        except mysql.connector.Error as err:
            LOG.error("Error closing cursor: %s" % err.msg)
        finally:
            # Closing the connection with the database server may lead to
            # exceptions but they are handled in such a way that they never lead
            # to an error disturbing normal operation. Closing the connection
            # can thus safely be done here.
            self.__cnx.close()

    def simple_query(self, query, params, condition=''):
        """
         Simple database query
         Input parameters:
           query - query number (one from QUERY_... constants)
           params - parameter tuple
                   when only one parameter is required specify as (parameter,)
         Return value:
             success:
               - First row of query result
             failure:
               - raise error
        """
        if self.__cnx is None:
            raise DbException.CONNECTION_FAILED
        try:
            complete_query = query.__add__(condition)
            self.__cursor.execute(complete_query, params)
            result = self.__cursor.fetchone()
            self.__cnx.commit()
            if result is None:
                raise DbException.QUERY_NO_RESULT
        except mysql.connector.Error:
            LOG.error("Query: '%s', parameters (%s)" % (complete_query, str(params)))
            raise DbException.MYSQL_CONNECTOR

        return result


    def log(self, ip, msg, resultcode, cid, stage):
        """ Logs CLIENT connection to the database using inform parameters """

        try:
            # Always [try to] log to the log table
            self.__cursor.execute(Queries.INSERT_CLIENTLOG,
                (cid, resultcode, ip, msg))
            self.__cursor.fetchone()
            self.__cnx.commit()

        except mysql.connector.Error as err:
            LOG.error("Database: Error during LOG, %s" % err.msg)
