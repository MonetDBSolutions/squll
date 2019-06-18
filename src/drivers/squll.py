"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel.

Author: M Kersten
This program is intended run the experiments against MonetDB.
It either contacts the sqapel website or uses a experiments.yaml file.

Accessing the website requires a session key obtained using a session
login to the website.
"""

import argparse
import time
import logging

from src.drivers.repository import Repository
from src.drivers.sqalpel import Sqalpel
from monetdb import MonetDB

parser = argparse.ArgumentParser(
    description='This program is the MonetDB experiment driver for SQALPEL.io. '
                'It requires an account on SQALPEL.io to obtain a task.'
                'With the task ticket you can obtain the details of a single task.'
                ''
                'The program should be started on each machine on which you want to perform an experiment. '
                'Offline experimentation against the baseline queries is supported for debugging as well.',
    formatter_class=argparse.HelpFormatter)


parser.add_argument('--ticket', type=str, help='Ticket', default='local')
parser.add_argument('--repository', type=str, help='Project Git', default='https://github.com/sqalpel/wisconsin.git')

parser.add_argument('--server', type=str, help='Sqalpel server URL', default='localhost:5000')

parser.add_argument('--db', type=str, help='Default database', default='wisconsin')
parser.add_argument('--dbms', type=str, help='Default DBMS', default='MonetDB')
parser.add_argument('--host', type=str, help='Default host', default='localhost')

parser.add_argument('--bailout', type=int, help='Abort after a number of errors', default=1)
parser.add_argument('--timeout', type=int, help='Abort lengthy experiments', default=None)

parser.add_argument('--daemon', help='Keep on polling the webserver', action='store_false')
parser.add_argument('--debug', help='Trace interaction', action='store_false')
parser.add_argument('--version', help='Show version info', action='store_true')


if __name__ == '__main__':
    args = parser.parse_args()
    if args.version:
        print('squll version 0.5')

    # The log information is gathered in a single file
    LOGFILE = time.strftime("%H:%M", time.localtime())
    logging.basicConfig(level=logging.INFO,
                        # filename= f'logs/{LOGFILE}',
                        # filemode='w',
                        format='%(levelname)-7s %(asctime)s  %(message)s',
                        datefmt='%H:%M:%S')

    # Connect to the sqalpel.io webserver for the real work
    sqalpel = Sqalpel(args)

    # Run the baseline queries locally for testing
    # This may require to set ranges for variables
    if args.ticket == 'local':
        if not args.repository:
            logging.error(f'Missing repository URL')
            exit(-1)

        # process the queries in the repository
        config = Repository.get_experiments(args.repository)
        if not config or 'experiments' not in config:
            logging.error(f'Invalid experiment configuration {args.repository}')
            exit(-1)
        experiments = config['experiments']

        results = []
        task = {'db': args.db,
                'dbms': args.dbms,
                'host': args.host,
                'params': '',
                'options': '{"runlength":1}'}

        for q in experiments:
            task.update({'query': q['source']})
            task.update({'xname': q['name']})
            # Before and After commands are optional
            if 'before' in q:
                task.update({'before': q['before']})
            else:
                task.update({'before': ''})
            if 'after' in q:
                task.update({'after': q['after']})
            else:
                task.update({'after': ''})
            sqalpel.prepare(task)

            # Use the database specific driver class
            MonetDB.run(sqalpel)
        exit(0)

    # the main purpose, repeatedly get work from the webserver
    delay = 5
    bailout = args.bailout

    while True:
        # If we don't get any work we either should stop or wait for it
        if sqalpel.get_work():
            # Use the database specific driver class
            MonetDB.run(sqalpel)
            if args.debug:
                logging.info(sqalpel.results)

        if sqalpel.error == 'Out of work' or sqalpel.error == 'Unknown task ticket':
            if not args.daemon or sqalpel.error == 'Unknown task ticket':
                break
            logging.info('Wait %d seconds for more work' % delay)
            time.sleep(delay)
            if delay < 60:
                delay += 5
            else:
                break
            continue
        elif sqalpel.error:
            if bailout == 0:
                logging.error('Bail out after too many database errors')
                break
            bailout -= 1

        if not sqalpel.put_work():
            if args.debug:
                logging.error(f'Error encountered in sending result: {sqalpel.error}')
            if not args.daemon:
                break

    if args.debug:
        print('Finished all the work')
