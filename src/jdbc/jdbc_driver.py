"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2018- MonetDB Solutions B.V.

Author: M Kersten

Execute a single query multiple times on the database nicknamed 'dbms'
and return a list of timings. The first error encountered stops the sequence.
The result is a dictionary with at least the structure {'times': [...]}

This driver merely assumes a simple jdbc driver connection.
Experimental, should be filled in.

"""

import time
import jaydebeapi

from abc import abstractmethod
from typing import Dict


class AbstractJDBCImplementation:

    def __init__(self, properties: Dict[str, str]):
        config_keys = ['uri', 'jars', 'user']
        for c in config_keys:
            if c not in properties:
                print('Configuration key "%s" not set in configuration file for target "%s"' % (
                        c, self.get_database_system_name()))
                exit(-1)
        self.uri = properties['uri']
        self.jars = [x.strip() for x in properties['jars'].split(',')]
        self.properties = self._compile_jdbc_properties(properties)
        self.properties['user'] = properties['user']
        pass

    @abstractmethod
    def get_database_system_name(self) -> str:
        pass

    @abstractmethod
    def get_java_driver_class(self) -> str:
        pass

    def get_jdbc_uri(self) -> str:
        return self.uri

    def get_jdbc_jars_path(self) -> [str]:
        return self.jars

    def get_jdbc_properties(self) -> Dict[str, str]:
        return self.properties

    @abstractmethod
    def _compile_jdbc_properties(self, properties: Dict[str, str]) -> Dict[str, str]:
        pass


class JDBCDriver:

    def __init__(self):
        pass

    @staticmethod
    def run(target, query, implementation: AbstractJDBCImplementation):
        """
        The number of repetitions is used to derive the best-of value.
        :param target:
        :param query:
        :param implementation: A JDBC implementation Python class
        :return:
        """
        db = target['db']
        jdbc_uri = implementation.get_jdbc_uri().format(database=db)
        repeat = int(target['repeat'])
        debug = target.getboolean('trace')
        response = {'error': '', 'times': [], 'cnt': [], 'clock': []}

        conn = None
        try:
            conn = jaydebeapi.connect(implementation.get_java_driver_class(),  # JDBC library class
                                      jdbc_uri,                                # JDBC uri
                                      implementation.get_jdbc_properties(),    # properties for the driver
                                      implementation.get_jdbc_jars_path())     # location of the jar
        except (Exception, jaydebeapi.DatabaseError) as msg:
            print('EXCEPTION :', msg)
            if conn is not None:
                conn.close()
                print('Database connection closed.')
            return response

        if debug:
            nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
            print('Run query:', nu, ':', query)

        for i in range(repeat):
            try:
                nu = time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())
                c = conn.cursor()
                ticks = time.time()
                c.execute(query)
                ticks = int((time.time() - ticks) * 1000)
                print('ticks', ticks)
                c.close()

            except Exception as msg:
                print('EXCEPTION :', i, msg)
                response['error'] = str(msg).replace("\n", " ").replace("'", "''")
                conn.close()
                return response

            response['times'].append(ticks)
            response['clock'].append(nu)

        conn.close()
        return response
