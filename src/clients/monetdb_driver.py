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
    times: [<ticks>]
    chks: [<integer value to represent result (e.g. cnt,  checksum or hash over result set) >]
    param: {param1:value1, ....}
    load : [<os load>]
    errors: []
    }]

If parameter value lists are given, we run the query for each element in the product.
"""

import re
import subprocess
import shlex
import time
import pymonetdb
import os
import itertools
import json


class MonetDBDriver:
    conn = None
    db = None

    def __init__(self):
        pass

    @staticmethod
    def startserver(db):
        if MonetDBDriver.conn:
            if MonetDBDriver.db == db:
                return None
            MonetDBDriver.stopserver()
        print('Start MonetDBDriver', db)
        try:
            MonetDBDriver.conn = pymonetdb.connect(database=db)
        except (Exception, pymonetdb.DatabaseError()) as msg:
            print('EXCEPTION ', msg)
            if MonetDBDriver.conn is not None:
                MonetDBDriver.close()
            return [{'error': json.dumps(msg), 'times': [], 'chks': [], 'param': []}]
        return None

    @staticmethod
    def stopserver():
        if not MonetDBDriver.conn:
            return None
        print('Stop MonetDBDriver')
        return None

    @staticmethod
    def run(task):
        """
        :param task:
        :return:
        """
        debug = task['debug']
        db = task['db']
        query = task['query']
        params = task['params']
        runlength = int(task['runlength'])

        response = []
        error = ''
        msg = MonetDBDriver.startserver(db)
        if msg:
            MonetDBDriver.stopserver()
            return msg

        if params:
            data = [json.loads(params[k]) for k in params.keys()]
            names = [d for d in params.keys()]
            gen = itertools.product(*data)
        else:
            gen = []
            names = []

        for z in gen:
            if error != '':
                break
            if debug:
                print('Run query:', time.strftime('%Y-%m-%d %H:%m:%S', time.localtime()))
                print('Parameter:', z)
                print(query)
            args = {}
            for n, v in zip(names, z):
                args.update({n: v})
            try:
                preload = ["%.3f" % v for v in list(os.getloadavg())]
            except os.error:
                preload = 0

            times = []
            chks = []
            newquery = query
            if z:
                print('args:', args)
                # replace the variables in the query
                for elm in args.keys():
                    newquery = re.sub(elm, str(args[elm]), newquery)
                print('New query', newquery)

            for i in range(runlength):
                try:
                    c = MonetDBDriver.conn.cursor()

                    ticks = time.time()
                    c.execute(newquery)
                    r = c.fetchone()
                    if r:
                        chks.append(int(r[0]))
                    else:
                        chks.append('')
                    times.append(int((time.time() - ticks) * 1000))

                    if debug:
                        print('ticks[%s]' % i, times[-1])
                    c.close()

                except (Exception, pymonetdb.DatabaseError) as msg:
                    print('EXCEPTION ', msg)
                    error = str(msg).replace("\n", " ").replace("'", "''")
                    break

            # wrapup the experimental runs
            try:
                postload = ["%.3f" % v for v in list(os.getloadavg())]
            except os.error:
                postload = 0
            res = {'times': times,
                   'chks': chks,
                   'param': args,
                   'error': error,
                   'load': str(preload + postload).replace("'", "")}
            response.append(res)

        MonetDBDriver.stopserver()
        return response
