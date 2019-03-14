"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M. Kersten, T Gubner

Execute a single query multiple times on the database nicknamed 'dbms'
and return a list of timings. The first error encountered stops the sequence.
The result is a dictionary with at least the structure {'times': [...]}

Use the command line tool mclient to gather more details.
"""

import re
import subprocess
import shlex
import time
import tempfile
import datetime
import os

class ActianClientDriver:

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
        timeout = int(target['timeout'])
        debug = False # target.getboolean('debug')
        response = {'error': '', 'times': [], 'cnt': [], 'clock': [], 'extra':[]}
        try:
            preload = [ "%.3f" % v for v in list(os.getloadavg())]
        except os.error:
            preload = 0
            pass

        action = 'sql "{database}"'
        action = target['command']

        z = action.format(database=db)
        args = shlex.split(z)

        # Retrieve output for post analysis
        out = subprocess.PIPE
        err = subprocess.STDOUT

        if debug:
            nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
            print('Run query:', nu, ':',  query)

        for i in range(runlength):
            with tempfile.TemporaryFile() as queryfile:
                queryfile.write(query.encode('utf-8'))
                queryfile.write("\\g".encode('utf-8'))
                queryfile.seek(0)

                try:
                    nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
                    ms = datetime.datetime.now()
                    proc = subprocess.run(args, timeout=timeout, check=True, stdin=queryfile, stdout=out, stderr=err)
                    response['answer'] = 'No answer'
                    ms = datetime.datetime.now() - ms
                except subprocess.SubprocessError as msg:
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
        try:
            postload = [ "%.3f" % v for v in list(os.getloadavg())]
        except os.error:
            postload = 0
            pass
        response['cpuload'] = str(preload + postload).replace("'", "")
        return response
