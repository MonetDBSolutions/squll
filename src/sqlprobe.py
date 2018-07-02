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

parser = argparse.ArgumentParser(
    description='SQLprobe is the experiment driver for SQLscalpel. '
                'It should be started on each machine you want to experiment with. '
                'The details of running the experiment script can be kept private. '
                'However, the database/query text is provided by the server and the results '
                'are identified by the target name for comparison later.',
    epilog='''For more information see: [paper]''',
    formatter_class=argparse.HelpFormatter)


parser.add_argument('--config', type=str, help='Configuration file to use', default='sqlprobe.conf')
parser.add_argument('--target', type=str, help='Target system to use', default='DEFAULT')
parser.add_argument('--version', help='Show version info', action='store_true')


if __name__ == '__main__':
    args = parser.parse_args()
    if args.version:
        print('SQLprobe Version 0.1')

    # the configuration file is consider local
    config = configparser.ConfigParser()
    try:
        config.read(args.config)
    except :
        print('Could not find the configuration file')
        exit(-1)

    if args.target not in config:
        print("Could not find the taget section '%s' in the configuration file '%s'" %
              (args.target, args.config))
        exit(-1)
    focus = config[args.target]

    if args.target == 'DEFAULT':
        print('CONFIG SECTIONS', config.sections())
        print('CONFIG TARGET', args.target)
        exit(0)

    # sanity check on the configuration file
    configkeys = ['server', 'user', 'db', 'dbms', 'host',
                  'project', 'experiment', 'repeat', 'batch', 'debug', 'timeout', ]
    for c in configkeys:
        if c not in focus:
            print('Configuration key "%s" not set in configuration file for target "%s"' % (c, args.target))
            exit(-1)

    # Connect to the SQLscalpel webserver
    conn = Connection(focus)

    dblist = focus['db'].split(',').copy()
    if len(dblist) > 1:
        print('Databases:', dblist)
    xlist = focus['experiment'].split(',').copy()
    if len(xlist) > 1:
        print('Experiments:', xlist)

    doit = True
    delay = 5

    while doit:
        doit = False
        for db in dblist:
            focus['db'] = db
            for x in xlist:
                focus['experiment'] = x
                tasks = conn.get_work(focus)
                if tasks is None:
                    print('Lost connection with SQLscalpel server')
                    exit(-1)

                # If we don't get any work we either should stop or wait for it
                if tasks is None:
                    continue
                if len(tasks) > 0 and 'error' in tasks[0]:
                    print('Server reported an error:', tasks[0]['error'])
                    continue

                doit = doit or len(tasks) > 0
                for t in tasks:
                    if focus['dbms'].startswith('MonetDB'):
                        results = MonetDBClientDriver.run(focus, t['query'],)
                    # elif args.dbms.startswith('MonetDBlite'):
                    #   results = MonetDBliteDriver.run(focus, t['query'],)
                    elif focus['dbms'].startswith('PostgreSQL'):
                        results = PostgresDriver.run(focus, t['query'],)
                    elif focus['dbms'].startswith('Clickhouse'):
                        results = ClickhouseDriver.run(focus, t['query'],)
                    elif focus['dbms'].startswith('SQLite'):
                        results = SqliteDriver.run(focus, t['query'],)
                    else:
                        results = None
                        print('Undefined target platform', focus['dbms'])

                    if not conn.put_work(t, results, focus.getboolean('debug')):
                        print('Error encountered in sending result')
                        exit(0)
        if not doit and focus.getboolean('forever'):
            print('Wait %d seconds for more work' % delay)
            time.sleep(delay)
            if delay < 60:
                delay += 5
            doit = True
        else:
            delay = 5
    print('Finished all the work')
