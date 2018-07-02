"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2018- MonetDB Solutions B.V.

Author: M Kersten

Execute a single query multiple times on the database nicknamed 'dbms'
and return a list of timings. The first error encountered stops the sequence.
The result is a dictionary with at least the structure {'times': [...]}

This driver merely assumes a simple jdbc driver connection.
Experimental, should be filled in.

"""

import time
import jaydebeapi

class JDBCDriver:

    def __init__(self):
        pass

    @staticmethod
    def run(focus, query):
        """
        The number of repetitions is used to derive the best-of value.
        :param focus:
        :param query:
        :return:
        """
        db = focus['db']
        repeat = int(focus['repeat'])
        debug = focus.getboolean('trace')
        response = {'error': '', 'times': [], 'cnt': [], 'clock': []}

        conn = None
        try:
            conn = jaydebeapi.connect("org.hsqldb.jdbcDriver",  # JDBC library
                                      "jdbc:hsqldb:mem:.",      # url
                                      ["SA", ""],
                                      "/path/to/hsqldb.jar",)   # location of library
        except (Exception, jaydebeapi.DatabaseError) as msg:
            print('CONNECTION:', 'localhost', 5432, db)
            print('EXCEPTION :', msg)
            if conn is not None:
                conn.close()
                print('Database connection closed.')
            return response

        if debug:
            nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
            print('Run query:', nu, ':', query)

        for i in range(repeat):
            try:
                nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
                c = conn.cursor()
                ticks = time.time()
                c.execute(query)
                ticks = int((time.time() - ticks) * 1000)
                print('ticks', ticks)
                c.close()

            except (Exception, psycopg2.DatabaseError) as msg:
                print('EXCEPTION ', i, msg)
                response['error'] = str(msg).replace("\n", " ").replace("'", "''")
                return response

            response['times'].append(ticks)
            response['clock'].append(nu)
        return response
