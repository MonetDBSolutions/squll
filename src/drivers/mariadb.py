"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M. Kersten

The prototypical driver to run a Sqalpel experiment and report on it.

"""

import mysql
import logging


class MariaDB:

    @staticmethod
    def run(sqalpel):
        """
        :param sqalpel:
        :return:
        """

        # Establish a clean connection
        try:
            conn = mysql.connector.connect(port=sqalpel.target['port'], database=sqalpel.db, user='root')
        except (Exception, mysql.connector.DatabaseError()) as msg:
            sqalpel.error = msg
            logging.error(f"EXCEPTION {msg}")
            return

        # Collects all variants of an experiment
        for before, query, after in sqalpel.generate():
            # Process all experiments multiple times
            try:
                for i in range(sqalpel.runlength):
                    c = conn.cursor()
                    if before:
                        c.execute(before)

                    sqalpel.start()
                    c.execute(query)
                    sqalpel.done()
                    try:
                        # if we have a result set, then obtain first row to represent it
                        r = c.fetchone()
                        if r:
                            sqalpel.keep(r)
                        else:
                            sqalpel.keep('')
                    except (Exception, mysql.connector.DatabaseError) as e:
                        sqalpel.error = e

                    if after:
                        c.execute(after)

                    c.close()

            except (Exception, mysql.connector.DatabaseError) as msg:
                logging.error(f'EXCEPTION  {msg}')
                sqalpel.error = str(msg).replace("\n", " ").replace("'", "''")

        # Establish a clean wrapup
        try:
            conn.close()
        except (Exception,  mysql.connector.DatabaseError) as msg:
            sqalpel.error = msg
            logging.error(f"EXCEPTION {msg}")
