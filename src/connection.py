"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M Kersten
The tasks are obtained from the SQalpeL webservice using a separately supplied
authorization key.
"""
import requests
import json
import os


class Connection:
    # keep just enough information to contact the web server
    # and some session based
    server = 'localhost:5000'
    ticket = None
    memory = 0

    def __init__(self, section,):
        """
        Contact the sql server and gather some basic information of the platform to identify the platform results.
        :param newroot:
        :param key:
        :return:
        """

        self.server = section['server']
        self.ticket = section['ticket']
        self.timeout = section['timeout']
        self.debug = section.getboolean('debug')
        # construct password hash
        try:
            mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
            self.memory = mem_bytes / (1024. ** 3)
        except:
            pass

    def get_work(self, target):
        debug = target.getboolean('debug')

        endpoint = 'http://' + self.server + '/get_work'
        args = {'ticket': self.ticket}
        print('Ticket used', self.ticket)

        if target.getboolean('extras'):
            # also ask for the template and binding table
            args.update({'extras': ['template', 'binding']})

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
            print('Task received:', json.dumps(task, sort_keys=True, indent=4))

        task.update( {'timeout': self.timeout, 'debug': self.debug})
        return task

    def put_work(self, task, results, debug):
        if results is None:
            if debug:
                print('Missing result object')
            return None

        endpoint = 'http://' + self.server + '/put_work'
        u = { 'ticket': task['ticket'],
             'db': task['db'],  'dbms': task['dbms'],  'host': task['host'],
             'project': task['project'], 'experiment': task['experiment'], 'tag': task['tag'],
             'cpus': os.cpu_count(),
             'memory': self.memory,
             }
        u.update({'runs': results})
        response = ''
        try:
            if debug:
                print('sending', json.dumps(u, sort_keys=True, indent=4))
            response = requests.post(endpoint, json=u)
            print('Sent task result', response)
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print('REQUESTS exception', e)
            print('Failed to post to ', endpoint)
        print('Sent task result', response)
        return False
