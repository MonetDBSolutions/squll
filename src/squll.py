"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel.

Author: M Kersten
This program is intended to be ran on a target experimentation platform.
It contacts the SQalpeL.io webserver for tasks to be executed and
deploys a local (private) program to collect the measurements.

A number of pre-defined driver programs are included for inspiration.

"""

import argparse
import time
import yaml


from connection import Connection
import configparser
from clients.monetdb_driver import MonetDBDriver
# from .clients.monetdb_client_driver import MonetDBClientDriver
# from probe.monetdblite_driver import MonetDBliteDriver
from clients.clickhouse_driver import ClickhouseDriver
from clients.postgres_driver import PostgresDriver
from clients.sqlite_driver import SqliteDriver
from clients.actian_client_driver import ActianClientDriver
from clients.mariadb_driver import MariaDBDriver
from clients.firebird_driver import FirebirdDriver
from jdbc.jdbc_driver import JDBCDriver
from jdbc.jdbc_implementations import ApacheDerbyJDBCDriver, ApacheHiveJDBCDriver, H2JDBCDriver, HSQLDBJDBCDriver, \
    MonetDBLiteJDBCDriver

parser = argparse.ArgumentParser(
    description='Squll.py is the default experiment driver for SQALPEL.io. '
                'It requires an account on SQALPEL.io and being a member of the project team. '
                'To contribute results you have to obtain a task ticket.'
                'With the task ticket you can obtain the details of a single task.'
                ''
                'Squll.py should be started on each machine on which you want to perform an experiment. '
                'The configuration file contains sections with optional details for a specific run'
                'such by DB, DBMS and PLATFORM names'
                ''
                'Scripting is provided using the --get and --put parameters'
                ''
                'The result of a run should be reported to SQALPEL.io as REST call.'
                ''
                'The program can be easily modified to keep details of running the experiment private. ',

    formatter_class=argparse.HelpFormatter)


parser.add_argument('--config', type=str, help='Configuration file to use', default='squll.yaml')
parser.add_argument('--ticket', type=str, help='Ticket', default='public')
parser.add_argument('--timeout', type=int, help='Timeout in seconds', default=0)
parser.add_argument('--bailout', type=int, help='Stop after too many errors', default=0)
parser.add_argument('--deamon', help='Run as deamon', action='store_false')
parser.add_argument('--driver', type=str, help='Target driver', default=None)
parser.add_argument('--get', help='Get task', action='store_true')
parser.add_argument('--put', type=str, help='Put task response', default=None)
parser.add_argument('--debug', help='Trace interaction', action='store_false')
parser.add_argument('--version', help='Show version info', action='store_true')


if __name__ == '__main__':
    args = parser.parse_args()
    if args.version:
        print('squll version 0.5')
    config = None
    with open(args.config, 'r') as f:
        try:
            txt = f.read()
            config = yaml.load(txt)
        except ValueError as msg:
            print(f"Could not open '{args.config}'' ")
            exit(0)

    if args.driver not in config['drivers']:
        print(f'Unknown driver:{args.driver}')
        print('Known drivers ', [ n for n in config['drivers']])
        exit(0)

    section = config['drivers'][args.driver]

    # the command line overrules the default
    if args.debug:
        config['debug'] = args.debug
    if args.ticket:
        config['ticket'] = args.ticket
    if args.deamon:
        config['deamon'] = args.deamon
    if args.timeout:
        config['timeout'] = args.timeout
    if args.bailout:
        config['bailout'] = args.bailout

    if args.debug :
        print('DRIVER', section)
        print('TICKET', config['ticket'])
        print('TIMEOUT', config['timeout'])
        print('DEAMON', config['deamon'])

    # sanity check on the configuration file
    for c in ['server', 'ticket', 'bailout', 'debug', 'timeout', ]:
        if c not in config:
            print('Configuration key "%s" not set in configuration file for section "%s"' % (c, section))
            exit(-1)

    # Connect to the sqalpel.io webserver to get a single task
    conn = Connection(config)

    if args.put:
        print('Return a valid json string', args.put)
        exit(0)
    delay = 5
    bailout = 0

    while True:
        task = conn.get_work(section)
        if task is None:
            print('Lost connection with Sqalpel.io server')
            break

        # If we don't get any work we either should stop or wait for it
        if 'error' in task:
            print('Server reported an error:', task)
            if task['error'] == 'Out of work' or task['error'] == 'Unknown task ticket':
                if not config['daemon']:
                    break
                print('Wait %d seconds for more work' % delay)
                time.sleep(delay)
                if delay < 60:
                    delay += 5
                else:
                    break
                continue
            elif task['error']:
                bailout -= 1
                if bailout == 0:
                    print('Bail out after too many database server errors')
                    break
            continue

        if args.get:
            print(task)
            exit(0)

        if task['dbms'].lower() == 'monetdb':
            results = MonetDBDriver.run(task)
        elif task['dbms'].lower() == 'postgresql':
            results = PostgresDriver.run(task)
        elif task['dbms'].lower() == 'clickhouse':
            results = ClickhouseDriver.run(task)
        elif task['dbms'].lower() == 'sqlite':
            results = SqliteDriver.run(task)
        elif task['dbms'].lower() == 'actian':
            results = ActianClientDriver.run(task)
        elif task['dbms'].lower() == 'mariadb':
            results = MariaDBDriver.run(task)
        elif task['dbms'].lower() == 'firebird':
            results = FirebirdDriver.run(task)
        elif task['dbms'].lower() in ('apache derby', 'derby'):
            results = JDBCDriver.run(task, ApacheDerbyJDBCDriver(task))
        elif task['dbms'].lower() in ('apache hive', 'hive'):
            results = JDBCDriver.run(task, ApacheHiveJDBCDriver(task))
        elif task['dbms'].lower() == 'h2':
            results = JDBCDriver.run(task, H2JDBCDriver(task))
        elif task['dbms'].lower() == 'hsqldb':
            results = JDBCDriver.run(task, HSQLDBJDBCDriver(task))
        elif task['dbms'].lower() == 'monetdblite-java':
            results = JDBCDriver.run(task,  MonetDBLiteJDBCDriver(task))
        # elif args.dbms == 'MonetDBLite-Python'.lower():
        #   results = MonetDBliteDriver.run(task, t['query'],)
        else:
            results = None
            print('Undefined task platform', task['dbms'])

        print(results)
        if results and results[-1]['error'] != '':
            bailout -= 1
            if bailout == 0:
                print('Bail out after too many database errors')
                break
        if not conn.put_work(task, results, section.getboolean('debug')):
            print('Error encountered in sending result')
            if not section.getboolean('daemon'):
                break

    print('Finished all the work')
