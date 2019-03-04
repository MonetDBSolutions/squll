"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2018- MonetDB Solutions B.V.

Author: M Kersten

Execute a single query multiple times on the database nicknamed 'dbms'
and return a list of timings. The first error encountered stops the sequence.
The result is a dictionary with at least the structure {'times': [...]}
"""

import time
import sqlite3


class SqliteDriver:

    def __init__(self):
        pass

    @staticmethod
    def run(target, query):
        """
        The SQLite database name is derived from the Scalpel database name with extension .db
        :param target:
        :param query:
        :return:
        """
        db = target['db']
        runlength = int(target['runlength'])
        timeout = int(target['timeout'])
        debug = target.getboolean('trace')
        response = {'error': '', 'times': [], 'cnt': [], 'clock': []}

        conn = None
        try:
            conn = sqlite3.connect(target['dbfarm'] + db + '.db', timeout=timeout)
        except sqlite3.DatabaseError as msg:
            print('EXCEPTION ', msg)
            print(target['dbfarm'] + db + '.db')
            if conn is not None:
                conn.close()
                print('Database connection closed.')
            return response

        if debug:
            nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
            print('Run query:', nu, ':', query)

        for i in range(runlength):
            try:
                nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
                c = conn.cursor()
                ticks = time.time()
                c.execute(query)
                response['answer'] = 'No answer'
                ticks = int((time.time() - ticks) * 1000)
                print('ticks', ticks)
                c.close()
            except sqlite3.DatabaseError as msg:
                print('EXCEPTION ', i, msg)
                response['error'] = str(msg).replace("\n", " ").replace("'", "''")
                conn.close()
                return response

            response['times'].append(ticks)
            response['clock'].append(nu)

        conn.close()
        return response
