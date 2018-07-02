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

import re
import subprocess
import shlex
import time
import pymonetdb


class MonetDBDriver:

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
            conn = pymonetdb.connect()
            c = conn.cursor()
        except (Exception, pymonetdb.DatabaseError()) as msg:
            print('EXCEPTION ', msg)
            if conn is not None:
                conn.close()
                print('Database connection closed.')
            return

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

            except (Exception, pymonetdb.DatabaseError) as msg:
                print('EXCEPTION ', msg)
                response['error'] = str(msg).replace("\n", " ").replace("'","''")
                return None

            response['times'].append(ticks)
            response['clock'].append(nu)
        return response