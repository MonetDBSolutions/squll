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
    timeout = None
    debug = True
    memory = 0

    def __init__(self, config):
        """
        Contact the sql server and gather some basic information of the platform to identify the platform results.
        :param config:
        :return:
        """

        self.server = config['server']
        self.ticket = config['ticket']
        self.timeout = config['timeout']
        self.debug = config['debug']
        # construct password hash
        try:
            mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
            self.memory = mem_bytes / (1024. ** 3)
        except ValueError:
            pass

    def get_work(self, config):
        endpoint = 'http://' + self.server + '/get_work'
        args = {'ticket': self.ticket}
        if self.debug:
            print(f'Ticket used {self.ticket}')

        response = ''
        try:
            if self.debug:
                print('Endpoint', endpoint)
                print('Requesting', json.dumps(args, sort_keys=True, indent=4))
            response = requests.get(endpoint,  json=args, timeout=20)
        except requests.exceptions.RequestException as e:
            print('REQUESTS exception', e)
            if self.debug:
                print('WEB SERVER RESPONSE ', response)
            return None

        if response.status_code != 200:
            return None

        task = json.loads(response.content)
        if not task:
            print('No tasks available for the target section', json.dumps(args, sort_keys=True, indent=4))

        if 'extras' in task:
            print('extras', task['extras'])
            e = json.loads(task['extras'])
            task.update(e)

        if self.debug:
            print('Task received:', json.dumps(task, sort_keys=True, indent=4))
        return task

    def put_work(self, task, results):
        if results is None:
            if self.debug:
                print('Missing result object')
            return None

        endpoint = 'http://' + self.server + '/put_work'
        u = { 'ticket': task['ticket'],
              'db': task['db'],
              'dbms': task['dbms'],
              'host': task['host'],
              'project': task['project'],
              'experiment': task['experiment'],
              'tag': task['tag'],
             }
        u.update({'runs': results})
        response = ''
        try:
            if self.debug:
                print('sending', json.dumps(u, sort_keys=True, indent=4))
            response = requests.post(endpoint, json=u)
            if self.debug:
                print(f'Sent task result{response}')
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f'REQUESTS exception {e}')
            print(f'Failed to post to {endpoint}')
        print(f'Sent task result{response}')
        return False
