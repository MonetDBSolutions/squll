"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M Kersten

The prototypical driver to run a Sqalpel experiment and report on it.
"""

import logging
import sqlite3


class Sqlite:

    @staticmethod
    def run(sqalpel):
        """
        The SQLite database name is derived from the Scalpel database name with extension .db
        :param sqalpel:
        :return:
        """

        # Establish a clean connection
        try:
            conn = sqlite3.connect(sqalpel.target['dbfarm'] + sqalpel.db + '.db', timeout=sqalpel.timeout)
        except (Exception, sqlite3.DatabaseError()) as msg:
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
                    except (Exception, sqlite3.DatabaseError) as e:
                        sqalpel.error = e

                    if after:
                        c.execute(after)

                    c.close()

            except (Exception, sqlite3.DatabaseError) as msg:
                logging.error(f'EXCEPTION  {msg}')
                sqalpel.error = str(msg).replace("\n", " ").replace("'", "''")

        # Establish a clean wrapup
        try:
            conn.close()
        except (Exception,  sqlite3.DatabaseError) as msg:
            sqalpel.error = msg
            logging.error(f"EXCEPTION {msg}")