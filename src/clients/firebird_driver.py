"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2018- CWI

Author: M. Kersten

Execute a single query multiple times on the database nicknamed 'dbms'
and return a list of timings. The first error encountered stops the sequence.
The result is a dictionary with at least the structure {'times': [...]}
"""

import re
import subprocess
import shlex
import time
import tempfile
import configparser
import datetime
import fdb

class FirebirdDriver:

    def __init__(self):
        pass

    @staticmethod
    def run(target, query):
        """
        The number of repetitions is used to derive the best-of value.
        :param target:
        :param query:
        :return:
        """
        db = target['db']
        socket = target['dbsocket']
        repeat = int(target['repeat'])
        timeout = int(target['timeout'])
        debug = target.getboolean('debug')
        response = {'error': '', 'times': [], 'cnt': [], 'clock': [], 'extra':[]}

        conn = None
        try:
            fdb.connect(database = '/path/database.db', user = 'sysdba', password = 'pass')
        except (Exception, fdb.DatabaseError) as msg:
            print('Connection', target['port'], db)
            print('Exception', msg)
            if conn:
                conn.close()
                print('Database connection closed')
            return response

        if debug:
            nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
            print('Run query:', nu, ':',  query)

        for i in range(repeat):
            try:
                nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
                c= conn.cursor()
                ms = datetime.datetime.now()
                c.execute(query)
                ms = datetime.datetime.now() - ms
            except fdb.DatabaseError as msg:
                # a timeout should also stop the database process involved the hard way
                print('EXCEPTION ', i,  msg)
                response['error'] = str(msg).replace("\n", " ").replace("'", "''")
                return response

            if debug:
                print('response ', proc.stdout.decode('ascii')[:-1])

            response['times'].append(float(ms.microseconds) / 1000.0)
            response['cnt'].append(-1)  # not yet collected
            response['extra'].append([])
            response['clock'].append(nu)

        return response
