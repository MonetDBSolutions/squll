"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2016- MonetDB Solutions B.V.

Author: M Kersten
The tasks are obtained from a webservice. It does not itself provide a web service for inspection.
"""
import requests
import json
import statistics


class Connection:
    service = 'localhost:5000/'
    name = None
    user = None
    host = None
    dbms = None

    def __init__(self, focus,):
        """
        Contact the sql server and gather some basic information of the platform to identify the platform results.
        :param newroot:
        :param key:
        :return:
        """
        self.dbms = focus['dbms']
        self.host = focus['host']
        self.service = focus['server']
        self.user = focus['user']

    def get_work(self, focus):
        debug = focus.getboolean('debug')
        batch = int(focus['batch'])
        db = focus['db']
        if 'project' in focus:
            project = focus['project']
        else:
            project = 'all'
        if 'experiment' in focus:
            experiment = focus['experiment']
        else:
            experiment = all

        endpoint = 'http://' + self.service + '/get_work'
        args = {'user': self.user, 'host': self.host, 'dbms': self.dbms, 'db': db,
                'project': project, 'experiment': experiment, 'batch': batch, }
        if focus.getboolean('extras'):
            # also ask for the template and binding table
            args.update({'extras':'yes'})

        response = ''
        try:
            if debug:
                print('Endpoint', endpoint)
                print('Requesting', json.dumps(args, sort_keys=True, indent=4))
            response = requests.get(endpoint,  json=args, timeout=20)
        except:
            if debug:
                print('WEB SERVER RESPONSE ', response)
            return None

        if response.status_code != 200:
            return None
        task = json.loads(response.content)
        if debug:
            print('Task received:', task)

        return task

    def put_work(self, task, results, debug):
        if results is None:
            if debug:
                print('Missing result object')
            return None

        endpoint = 'http://' + self.service + '/put_work'
        u = {'usr': self.user, 'host': self.host, 'dbms': self.dbms,
             'tag': task['tag'], 'ptag': task['ptag'],
             'db': task['db'], 'project': task['project'], 'experiment': task['experiment'],
             'query': task['query'].replace("'","''"), 'exp': task['exp'],
             }

        results.update(u)
        response = ''
        try:
            if debug:
                print('sending', json.dumps(results, sort_keys=True, indent=4))
            response = requests.post(endpoint, json=results)
            return response.status_code == 200
        except:
            print('Failed to post to ', endpoint)
        if debug:
            print('Sent task result', response)
        return False
