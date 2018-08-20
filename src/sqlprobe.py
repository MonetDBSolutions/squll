"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2016- MonetDB Solutions B.V.

Author: M Kersten
This program is intended to be ran on a target experimentation platform.
It contacts the SQLscalpel webserver for tasks to be executed and
deploys a local (private) program to collect the measurements.

A number of pre-defined driver programs are included for inspiration.

"""

import argparse
import time
from connection import Connection
from monetdb_driver import MonetDBDriver
import configparser
from monetdb_client_driver import MonetDBClientDriver
# from probe.monetdblite_driver import MonetDBliteDriver
from clickhouse_driver import ClickhouseDriver
from postgres_driver import PostgresDriver
from sqlite_driver import SqliteDriver
from actian_client_driver import ActianClientDriver
from mariadb_driver import MariaDBDriver
from firebird_driver import FirebirdDriver
from jdbc.jdbc_driver import JDBCDriver
from jdbc.jdbc_implementations import ApacheDerbyJDBCDriver, ApacheHiveJDBCDriver, H2JDBCDriver, HSQLDBJDBCDriver, \
    MonetDBLiteJDBCDriver

parser = argparse.ArgumentParser(
    description='SQLprobe is the experiment driver for SQLscalpel. '
                'It requires an account on SQLscalpel and being a member of the team associated with a project.'
                'SQLprobe should be started on each machine you want to experiment with. '
                'The details of running the experiment can be kept private. '
                'However, the query text is provided by the server and the results '
                'are identified by the target name for comparison later.',
    epilog='''For more information see: [paper]''',
    formatter_class=argparse.HelpFormatter)


parser.add_argument('--config', type=str, help='Configuration file to use', default='sqlprobe.conf')
parser.add_argument('--target', type=str, help='Target system to use', default=None)
parser.add_argument('--stmt', type=str, help='Test query', default=None)
parser.add_argument('--offline', help='Just collect the queries', action='store_true')
parser.add_argument('--version', help='Show version info', action='store_true')


if __name__ == '__main__':
    args = parser.parse_args()
    if args.version:
        print('SQLprobe Version 0.2')

    # the configuration file is consider local
    config = configparser.ConfigParser()
    try:
        config.read(args.config)
    except:
        print('Could not find the configuration file')
        exit(-1)

    # print('CONFIG SECTIONS', config.sections())
    # print('CONFIG TARGET', args.target)
    target = None
    if args.target and args.target in config:
        target = config[args.target]
    elif 'target' in config['DEFAULT']:
        target = config[config['DEFAULT']['target']]

    if not target:
        print("Could not find the target section '%s' in the configuration file '%s'" %
              (args.target, args.config))
        exit(-1)

    # sanity check on the configuration file
    configkeys = ['home', 'server', 'key', 'db', 'dbms', 'host', 'bailout',
                  'project', 'experiment', 'repeat', 'debug', 'timeout', ]
    if not args.target:
        configkeys.append('target')
    for c in configkeys:
        if c not in target:
            print('Configuration key "%s" not set in configuration file for target "%s"' % (c, args.target))
            exit(-1)
    # Connect to the SQLscalpel webserver
    conn = None
    if not args.stmt:
        conn = Connection(target)

    dblist = target['db'].split(',')
    if len(dblist) > 1:
        print('Databases:', dblist)
    xlist = target['experiment'].split(',')
    if len(xlist) > 1:
        print('Experiments:', xlist)

    doit = True
    delay = 5

    while doit:
        doit = False
        for db in dblist:
            target['db'] = db
            for x in xlist:
                target['experiment'] = x
                if args.stmt:
                    tasks = [target]
                    tasks[0]['query'] = args.stmt
                else:
                    tasks = conn.get_work(target)
                if tasks is None:
                    print('Lost connection with SQLscalpel server')
                    if not target.getboolean('forever'):
                        exit(-1)
                if args.offline:
                    # collect all work in a local file for post processing.
                    for x in tasks:
                        print('task:',x)
                    continue

                # If we don't get any work we either should stop or wait for it
                if tasks is None:
                    continue
                if len(tasks) > 0 and 'error' in tasks[0]:
                    print('Server reported an error:', tasks[0]['error'])
                    bailout -= 1
                    if bailout == 0:
                        print('Bail out after too many database server errors')
                        exit(-1)
                    continue

                doit = doit or len(tasks) > 0
                for t in tasks:
                    if 'error' in t:
                        bailout -= 1
                        if bailout == 0:
                            print('Bail out after too many (%d) database errors' % bailout)
                            exit(-1)
                        continue

                    if target['dbms'].lower() == 'monetdb':
                        results = MonetDBClientDriver.run(target, t['query'], )
                    elif target['dbms'].lower() == 'postgresql':
                        results = PostgresDriver.run(target, t['query'],)
                    elif target['dbms'].lower() == 'clickhouse':
                        results = ClickhouseDriver.run(target, t['query'],)
                    elif target['dbms'].lower() == 'sqlite':
                        results = SqliteDriver.run(target, t['query'],)
                    elif target['dbms'].lower() == 'actian':
                        results = ActianClientDriver.run(target, t['query'], )
                    elif target['dbms'].lower() == 'mariadb':
                        results = MariaDBDriver.run(target, t['query'], )
                    elif target['dbms'].lower() == 'firebird':
                        results = FirebirdDriver.run(target, t['query'], )
                    elif target['dbms'].lower() in ('apache derby', 'derby'):
                        results = JDBCDriver.run(target, t['query'], ApacheDerbyJDBCDriver(target))
                    elif target['dbms'].lower() in ('apache hive', 'hive'):
                        results = JDBCDriver.run(target, t['query'], ApacheHiveJDBCDriver(target))
                    elif target['dbms'].lower() == 'h2':
                        results = JDBCDriver.run(target, t['query'], H2JDBCDriver(target))
                    elif target['dbms'].lower() == 'hsqldb':
                        results = JDBCDriver.run(target, t['query'], HSQLDBJDBCDriver(target))
                    elif target['dbms'].lower() == 'monetdblite-java':
                        results = JDBCDriver.run(target, t['query'], MonetDBLiteJDBCDriver(target))
                    # elif args.dbms == 'MonetDBLite-Python'.lower():
                    #   results = MonetDBliteDriver.run(target, t['query'],)
                    else:
                        results = None
                        print('Undefined target platform', target['dbms'])

                    if results['error'] != '':
                        bailout -= 1
                        if bailout == 0:
                            print('Bail out after too many database target errors')
                            exit(-1)
                    if args.stmt:
                        print('result', dict(t), results)
                    elif not conn.put_work(t, results, target.getboolean('debug')):
                        print('Error encountered in sending result')
                        if not target.getboolean('forever'):
                            exit(0)
        if args.stmt:
            break
        if not doit and target.getboolean('forever'):
            print('Wait %d seconds for more work' % delay)
            time.sleep(delay)
            if delay < 60:
                delay += 5
            doit = True
        else:
            delay = 5
    print('Finished all the work')
