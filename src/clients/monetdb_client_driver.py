"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2018- MonetDB Solutions B.V.

Author: M Kersten

Execute a single query multiple times on the database nicknamed 'dbms'
and return a list of timings. The first error encountered stops the sequence.
The result is a dictionary with at least the structure {'times': [...]}

Use the command line tool mclient to gather more details.
"""

import re
import subprocess
import shlex
import time


class MonetDBClientDriver:

    def __init__(self):
        pass

    @staticmethod
    def run(task):
        """
        The number of repetitions is used to derive the best-of value.
        :param task:
        :param query:
        :return:
        """
        db = task['db']
        query = task['query']
        runlength = int(task['runlength'])
        timeout = int(task['timeout'])
        debug = task['debug']
        response = {'error': '', 'times': [], 'cnt': [], 'clock': [], 'extra':[]}

        action = 'mclient -d {database} -tperformance -ftrash -s "{query}"'

        z = action.format(query=query, database=db)
        args = shlex.split(z)

        # Retrieve output for post analysis
        out = subprocess.PIPE
        err = subprocess.STDOUT

        query = ' '.join(args)
        if debug:
            nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
            print('Run query:', nu, ':',  query)

        for i in range(runlength):
            try:
                nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
                proc = subprocess.run(args, timeout=timeout, check=True, stdout=out, stderr=err)
                response['answer'] = 'No answer'
            except subprocess.SubprocessError as msg:
                # a timeout should also stop the database process involved the hard way
                print('EXCEPTION ', i,  msg)
                response['error'] = str(msg).replace("\n", " ").replace("'", "''")
                return response

            if debug:
                print('response ', proc.stdout.decode('ascii')[:-1])

            # for the build in drivers we can analyse the result directly for the response time
            runtime = re.compile('(.*run:)(\d+\.\d+)(.*)')
            optimizer = re.compile('(.*opt:)(\d+\.\d+)(.*)')
            sqlparse = re.compile('(.*sql:)(\d+\.\d+)(.*)')
            if runtime.match(str(proc.stdout)) is None:
                # error detected
                response['error'] = str(proc.stdout).replace("'", "''")
                response['error'] = response['error'][response['error'].find('ERROR ') + 8:]
                break
            ms = float(runtime.match(str(proc.stdout)).group(2))
            opt = float(optimizer.match(str(proc.stdout)).group(2))
            sql = float(sqlparse.match(str(proc.stdout)).group(2))

            response['times'].append(ms + opt + sql)
            response['cnt'].append(-1)  # not yet collected
            response['extra'].append([ms,opt,sql])
            response['clock'].append(nu)

        return response
