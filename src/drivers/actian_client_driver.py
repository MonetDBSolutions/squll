"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M. Kersten, T Gubner

The prototypical driver to run a Sqalpel experiment and report on it.
"""

import re
import subprocess
import shlex
import time
import tempfile
import datetime
import os


class ActianClientDriver:
    conn = None
    db = None

    def __init__(self):
        pass

    @staticmethod
    def startserver(db):
        if ActianDriver.conn:
            # avoid duplicate connection
            if ActianDriver.db == db:
                return None
            ActianDriver.stopserver()
        print('Start ActianDriver', db)
        try:
            ActianDriver.conn = pymonetdb.connect(database=db)
        except os.error as msg:
            print('EXCEPTION ', msg)
            if ActianDriver.conn is not None:
                ActianDriver.close()
            return [{'error': json.dumps(msg), 'times': [], 'chks': [], 'param': []}]
        return None

    @staticmethod
    def stopserver():
        if not ActianDriver.conn:
            return None
        print('Stop ActianDriver')
        # to be implemented
        return None

    @staticmethod
    def run(task):
        """
        The number of repetitions is used to derive the best-of value.
        :param task:
        :return:
        """
        debug = task.getboolean('debug')
        db = task['db']
        query = task['query']
        params = task['params']
        options = json.loads(task['options'])
        if 'runlength' in options:
            runlength = int(options['runlength'])
        else:
            runlength = 1
        print('runs', runlength)

        response = []
        error = ''
        msg = ActianDriver.startserver(db)
        if msg:
            ActianDriver.stopserver()
            return msg

        if params:
            data = [json.loads(params[k]) for k in params.keys()]
            names = [d for d in params.keys()]
            gen = itertools.product(*data)
        else:
            gen = [[1]]
            names = ['_ * _']

        # Retrieve output for post analysis
        out = subprocess.PIPE
        err = subprocess.STDOUT
        for z in gen:
            if error != '':
                break
            if debug:
                print('Run query:', time.strftime('%Y-%m-%d %H:%m:%S', time.localtime()))
                print('Parameter:', z)
                print(query)

            args = {}
            for n, v in zip(names, z):
                if params:
                    args.update({n: v})
            try:
                preload = [v for v in list(os.getloadavg())]
            except os.error:
                preload = 0

            times = []
            chks = []
            newquery = query
            if z:
                if debug:
                    print('args:', args)
                # replace the variables in the query
                for elm in args.keys():
                    newquery = re.sub(elm, str(args[elm]), newquery)
                print('New query', newquery)
            newquery = newquery + ' limit 1'
            if debug:
                print('Run query:', nu, ':',  newquery)

            #  action = 'sql "{database}"'
            action = newquery

            z = action.format(database=db)
            args = shlex.split(z)

            for i in range(runlength):
                with tempfile.TemporaryFile() as queryfile:
                    queryfile.write(query.encode('utf-8'))
                    queryfile.write("\\g".encode('utf-8'))
                    queryfile.seek(0)
    
                    try:
                        ticks = time.time()
                        proc = subprocess.run(args, timeout=timeout, check=True,
                                              stdin=queryfile, stdout=out, stderr=err)
                        r = proc.stdout.decode('ascii')[:-1]
                        if debug:
                            print('response ', r)
                        if r:
                            chks.append(int(r[0]))
                        else:
                            chks.append('')
                        times.append(int((time.time() - ticks) * 1000))

                        if debug:
                            print('ticks[%s]' % i, times[-1])
                        proc.close()
                    except subprocess.SubprocessError as msg:
                        # a timeout should also stop the database process involved the hard way
                        print('EXCEPTION ', i,  msg)
                        error = str(msg).replace("\n", " ").replace("'", "''")
                        break

            # wrapup the experimental runs,
            # The load can be sent as something extra, it is an internal metric
            try:
                postload = [v for v in list(os.getloadavg())]
            except os.error:
                postload = 0

            res = {'times': times,
                   'chksum': chks,
                   'param': args,
                   'error': error,
                   'metrics': {'load': preload + postload},
                   }

            response.append(res)
        if debug:
            print('Finished the run')
        ActianDriver.stopserver()
        return response
