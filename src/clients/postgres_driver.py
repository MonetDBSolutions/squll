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

import time
import psycopg2
import os


class PostgresDriver:
    conn = None
    db = None

    def __init__(self):
        pass

    @staticmethod
    def startserver(db):
        if PostgresqlDriver.conn:
            # avoid duplicate connection
            if PostgresqlDriver.db == db:
                return None
            PostgresqlDriver.stopserver()
        print('Start PostgresqlDriver', db)
        try:
            PostgresqlDriver.conn = psycopg2.connect(host='localhost', port=5432, database=db)
        except (Exception, psycopg2.DatabaseError()) as msg:
            print('EXCEPTION ', msg)
            if PostgresqlDriver.conn is not None:
                PostgresqlDriver.close()
            return [{'error': json.dumps(msg), 'times': [], 'chks': [], 'param': []}]
        return None

    @staticmethod
    def stopserver():
        if not PostgresqlDriver.conn:
            return None
        print('Stop PostgresqlDriver')
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
        msg = PostgresqlDriver.startserver(db)
        if msg:
            PostgresqlDriver.stopserver()
            return msg

        if params:
            data = [json.loads(params[k]) for k in params.keys()]
            names = [d for d in params.keys()]
            gen = itertools.product(*data)
        else:
            gen = [[1]]
            names = ['_ * _']
            
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
                
            for i in range(repeat):                
                try:
                    c = PostgresqlDriver.conn.cursor()
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
                except (Exception, psycopg2.DatabaseError) as msg:
                    print('EXCEPTION ', msg)
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
        PostgresqlDriver.stopserver()
        return response
