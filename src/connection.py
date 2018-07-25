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
import os


class Connection:
    server = 'localhost:5000/'
    name = None
    user = None
    passwd = None
    host = None
    dbms = None
    version = None
    tasks = None
    preload = None
    postload = None
    memory = -1

    def __init__(self, target,):
        """
        Contact the sql server and gather some basic information of the platform to identify the platform results.
        :param newroot:
        :param key:
        :return:
        """
        self.dbms = target['dbms']
        self.version = target['version']
        self.host = target['host']
        self.server = target['server']
        self.user = target['user']
        # construct password hash
        try:
            mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
            self.memory = mem_bytes / (1024. ** 3)
        except:
            pass
        if 'input' in target and target['input']:
            # read the input file with experiment records and process them one by one
            with open(target['input'], 'r') as f:
                self.tasks = json.loads(f.read())
                f.close()
                print('Restored a batch of %d tasks', len(self.tasks))

    def get_work(self, target):
        debug = target.getboolean('debug')

        # read from the batch file first
        if self.tasks and 'input' in target:
            t = self.tasks[0]
            self.tasks.remove(0)
            return t
        if self.tasks == []:
            if debug:
                print('no elements in local task queue')
            return None

        if 'db' in target:
            db = target['db']
        else:
            db = '*'
        if 'project' in target:
            project = target['project']
        else:
            project = '*'
        if 'experiment' in target:
            experiment = target['experiment']
        else:
            experiment = '*'

        endpoint = 'http://' + self.server + '/get_work'
        args = {'user': self.user, 'host': self.host, 'dbms': self.dbms, 'version': self.version, 'db': db,
                'project': project, 'experiment': experiment, 'passwordhash': 0}
        if target.getboolean('extras'):
            # also ask for the template and binding table
            args.update({'extras': 'yes'})

        response = ''
        try:
            if debug:
                print('Endpoint', endpoint)
                print('Requesting', json.dumps(args, sort_keys=True, indent=4))
            response = requests.get(endpoint,  json=args, timeout=20)
        except requests.exceptions.RequestException as e:
            print('REQUESTS exception', e)
            if debug:
                print('WEB SERVER RESPONSE ', response)
            return None

        if response.status_code != 200:
            return None
        task = json.loads(response.content)
        if not task:
            print('No tasks available for the target section', json.dumps(args, sort_keys=True, indent=4))
        if debug:
            print('Task received:', task)
        self.preload = [ "%.3f" % v for v in list(os.getloadavg())]
        return task

    def put_work(self, task, results, debug):
        if results is None:
            if debug:
                print('Missing result object')
            return None
        try:
            self.postload = [ "%.3f" % v for v in list(os.getloadavg())]
        except os.error:
            pass
        endpoint = 'http://' + self.server + '/put_work'
        u = {'exp': task['exp'], 'tag': task['tag'], 'ptag': task['ptag'],
             'usr': self.user, 'host': self.host, 'dbms': self.dbms, 'version': self.version,
             'db': task['db'], 'project': task['project'], 'experiment': task['experiment'],
             'query': task['query'].replace("'", "''"),
             'cpucount': os.cpu_count(), 'cpuload': str(self.preload + self.postload).replace("'",""), 'ram': self.memory,
             }

        results.update(u)
        response = ''
        try:
            if debug:
                print('sending', json.dumps(results, sort_keys=True, indent=4))
            response = requests.post(endpoint, json=results)
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print('REQUESTS exception', e)
            print('Failed to post to ', endpoint)
        if debug:
            print('Sent task result', response)
        return False
