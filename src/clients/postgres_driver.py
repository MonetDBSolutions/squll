"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M Kersten

Execute a single query multiple times on the database nicknamed 'dbms'
and return a list of timings. The first error encountered stops the sequence.
The result is a dictionary with at least the structure {'times': [...]}

"""

import time
import psycopg2
import os


class PostgresDriver:

    def __init__(self):
        pass

    @staticmethod
    def run(target):
        """
        The number of repetitions is used to derive the best-of value.
        :param target:
        :return:
        """
        db = target['db']
        query = target['query']
        params = target['params']
        runlength = int(target['runlength'])

        debug = target.getboolean('trace')
        response = {'error': '', 'times': [], 'cnt': [], 'clock': []}
        try:
            preload = [ "%.3f" % v for v in list(os.getloadavg())]
        except os.error:
            preload = 0
            pass

        conn = None
        try:
            conn = psycopg2.connect(host='localhost', port=5432, database=db)
        except (Exception, psycopg2.DatabaseError) as msg:
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
                response['answer'] = 'No answer'
                ticks = int((time.time() - ticks) * 1000)
                print('ticks', ticks)
                c.close()
            except (Exception, psycopg2.DatabaseError) as msg:
                print('EXCEPTION ', i, msg)
                response['error'] = str(msg).replace("\n", " ").replace("'", "''")
                conn.close()
                return response

            response['times'].append(ticks)
            response['clock'].append(nu)
        try:
            postload = [ "%.3f" % v for v in list(os.getloadavg())]
        except os.error:
            postload = 0
            pass
        response['cpuload'] = str(preload + postload).replace("'", "")
        conn.close()
        return response
