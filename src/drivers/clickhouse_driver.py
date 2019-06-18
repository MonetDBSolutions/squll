"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M Kersten

Execute a single query multiple times on the database nicknamed 'db'
and return a list of timings. The first error encountered aborts the sequence.
The result is a list of dictionaries
run: [{
    times: [<response time>]
    chks: [<integer value to represent result (e.g. cnt,  checksum or hash over result set) >]
    param: {param1:value1, ....}
    errors: []
    }]

If parameter value lists are given, we run the query for each element in the product.

Internal metrics, e.g. cpu load, is returned as a JSON structure in 'metrics' column
"""

import re
import subprocess
import shlex
import time
import os


class ClickhouseDriver:

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
        runlength = int(target['runlength'])
        timeout = int(target['timeout'])
        debug = target.getboolean('debug')
        response = {'error': '', 'times': [], 'clock': []}
        try:
            preload = ["%.3f" % v for v in list(os.getloadavg())]
        except os.error:
            preload = 0
            pass

        action = 'clickhouse client --time --format=Null --query="{query}"',

        z = action.format(query=query, database=db)
        args = shlex.split(z)

        # Retrieve output for post analysis
        out = subprocess.PIPE
        err = subprocess.STDOUT

        query = ' '.join(args)
        if debug:
            nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
            print('Run query:', nu, ':', query)

        for i in range(runlength):
            try:
                nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
                proc = subprocess.run(args, timeout=timeout, check=True, stdout=out, stderr=err)
                response['answer'] = 'No answer'
            except subprocess.SubprocessError as msg:
                print('EXCEPTION ', i, msg)
                response['error'] = str(msg).replace("\n", " ").replace("'", "''")
                return response

            if debug:
                print('response ', proc.stdout.decode('ascii')[:-1])

            # for the build in drivers we can analyse the result directly for the response time
            runtime = re.compile('(.*)(\d+\.\d+)(.*)')

            if runtime.match(str(proc.stdout)) is None:
                # error detected
                response['error'] = str(proc.stdout)
                break
            ms = float(runtime.match(str(proc.stdout)).group(2))
            response['times'].append(ms)
            response['clock'].append(nu)
        try:
            postload = ["%.3f" % v for v in list(os.getloadavg())]
        except os.error:
            postload = 0
            pass
        response['cpuload'] = str(preload + postload).replace("'", "")
        return response
