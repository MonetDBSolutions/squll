"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M Kersten
The code to contact the Sqalpel webserver is organized here.

Execute a single query multiple times on the database nicknamed 'sqalpel.db'
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
import requests
import json
import os
import re
import time
import itertools
import logging


class Sqalpel:
    # keep enough state information to contact the web server
    # and task specific information
    debug = False
    ticket = None
    server = 'localhost:5000'
    db = None
    dbms = None
    host = None
    timeout = None
    memory = 0
    runlength = 1
    prelude = None
    postlude = None
    task = None
    options = None
    error = None
    data = None
    names = None
    gen = None
    args = None
    ticks = None
    times = []
    chks = []
    metrics = {}
    results = []

    def __init__(self, args):
        """
        Contact the sql server and gather some basic information of the platform to identify the platform results.
        :param args:
        :return:
        """
        logging.info(f"{args}")
        self.server = args.server
        self.ticket = args.ticket
        self.timeout = args.timeout
        self.debug = args.debug
        if 'runlength' in args:
            self.runlength = int(args['runlength'])
        if 'prelude' in args:
            self.prelude = args['prelude']
        if 'postlude' in args:
            self.postlude = args['postlude']
        try:
            mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
            self.memory = mem_bytes / (1024. ** 3)
        except ValueError:
            pass

    def prepare(self, task):
        logging.info(f"PREPARE {task}")
        if 'error' in task:
            self.error = task['error']
            return

        self.task = task
        self.error = None
        self.db = task['db']
        self.dbms = task['dbms']
        self.host = task['host']
        self.prelude = task['prelude']
        self.postlude = task['postlude']
        if 'extras' in self.task:
            e = json.loads(self.task['extras'])
            self.task.update(e)
        self.options = json.loads(task['options'])
        self.results = []

        if task['params']:
            self.data = [json.loads(task['params'][k]) for k in task['params'].keys()]
            self.names = [d for d in task['params'].keys()]
            self.gen = itertools.product(*self.data)
        else:
            self.gen = [[1]]
            self.names = ['_ * _']
        logging.info(f"prepare {self.task}")

    def keep(self, res):
        self.chks.append(res)

    def start(self):
        self.ticks = time.time()

    def done(self):
        self.times.append(int((time.time() - self.ticks) * 1000))
        if self.debug:
            print(f"ticks {self.times[-1]}")

    def generate(self):
        logging.info(f"{self.gen}, {self.error}, {self.debug}")
        for z in self.gen:
            if self.error:
                break
            if self.debug:
                logging.info(f'Parameter: {z}')
                logging.info(self.task['query'])

            self.args = {}
            for n, v in zip(self.names, z):
                if self.task['params']:
                    self.args.update({n: v})
            self.times = []
            self.chks = []

            newquery = self.task['query']
            newbefore = self.prelude
            newafter = self.postlude
            if z:
                # replace the variables in the query
                for elm in self.args.keys():
                    newquery = re.sub(elm, str(self.args[elm]), newquery)
                    if newbefore:
                        newbefore = re.sub(elm, str(self.args[elm]), newbefore)
                    if newafter:
                        newafter = re.sub(elm, str(self.args[elm]), newafter)
                if self.debug:
                    if newbefore:
                        logging.info(f'Before {newbefore}')
                    logging.info(f'Query {newquery}')
                    if newafter:
                        logging.info(f'After {newafter}')
            try:
                # Collect some system metrics
                preload = [v for v in list(os.getloadavg())]
            except os.error:
                preload = 0
            self.metrics = {'load': preload}

            if self.debug:
                logging.info(f"Run {self.task}")

            if self.debug:
                logging.info(f"EXECUTE BEFORE {newbefore}")
                logging.info(f"EXECUTE QUERY  {newquery}")
                logging.info(f"EXECUTE AFTER  {newafter}")

            yield newbefore, newquery, newafter
            res = {'times': self.times,
                   'chksum': self.chks,
                   'param': self.args,
                   'error': self.error,
                   'metrics': self.metrics
                   }
            if self.chks:
                self.results.append(res)
                self.ticks = None
                self.times = []
                self.chks = []

    def get_work(self):
        logging.info(f"{self.server}")
        self.error = None
        self.task = None

        endpoint = 'http://' + self.server + '/get_work'
        args = {'ticket': self.ticket}
        if self.debug:
            logging.info(f'Ticket used {self.ticket}')

        try:
            if self.debug:
                logging.info(f'Endpoint {endpoint}')
                logging.info(f'Requesting {json.dumps(args, sort_keys=True, indent=4)}')
            response = requests.get(endpoint,  json=args, timeout=20)
        except requests.exceptions.RequestException as e:
            self.error = e
            logging.error(f'REQUESTS exception {e}')
            return

        if response.status_code != 200:
            return

        task = json.loads(response.content)
        if not task:
            logging.info(f'No tasks available for the target section {json.dumps(args, sort_keys=True, indent=4)}')
        else:
            self.prepare(task)
            if self.debug:
                logging.info(f'Task received: {json.dumps(self.task, sort_keys=True, indent=4)}')

    def put_work(self):
        if not self.results:
            self.error = 'Missing result object'
            if self.debug:
                logging.info('Missing result object')
            return None

        endpoint = 'http://' + self.server + '/put_work'
        # prepare the answer for the webserver
        u = {'ticket': self.task['ticket'],
             'db': self.task['db'],
             'dbms': self.task['dbms'],
             'host': self.task['host'],
             'project': self.task['project'],
             'experiment': self.task['experiment'],
             'tag': self.task['tag'],
             }
        u.update({'runs': self.results})

        # move the message to the webserver
        response = ''
        try:
            if self.debug:
                logging.info(f'sending {json.dumps(u, sort_keys=True, indent=4)}')
            response = requests.post(endpoint, json=u)
            if self.debug:
                logging.info(f'Sent task result{response}')
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            self.error = e
            logging.error(f'REQUESTS exception {e}')
            logging.error(f'Failed to post to {endpoint}')
        logging.info(f'Sent task result{response}')
        return False
