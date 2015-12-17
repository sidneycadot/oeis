#! /usr/bin/env python3

import os
import sys
import time
import logging
import sqlite3
import numpy as np
from matplotlib import pyplot as plt
from exit_scope    import close_when_done
from setup_logging import setup_logging

logger = logging.getLogger(__name__)

def show_entries(database_filename):

    if not os.path.exists(database_filename):
        logger.critical("Database file '{}' not found! Unable to continue.".format(database_filename))
        return

    t_current = time.time()

    with close_when_done(sqlite3.connect(database_filename)) as dbconn, close_when_done(dbconn.cursor()) as dbcursor:
        query = "SELECT oeis_id, t1, t2 FROM oeis_entries;"
        dbcursor.execute(query)
        data = dbcursor.fetchall()

    data_dtype = np.dtype([
            ("oeis_id", np.int),
            ("t1"     , np.float64),
            ("t2"     , np.float64)
        ]
    )

    data = np.array(data, dtype = data_dtype)

    t1      = data["t1"]
    t2      = data["t2"]
    oeis_id = data["oeis_id"]

    age       = t_current - t2
    stability = t2 - t1

    priority = age / np.maximum(stability, 1e-6)

    plt.subplot(331)
    plt.xlabel("oeis id")
    plt.ylabel("t1 [h]")
    plt.plot(oeis_id, (t1 - t_current) / 3600.0, '.', markersize = 0.5)

    plt.subplot(332)
    plt.xlabel("oeis id")
    plt.ylabel("t2 [h]")
    plt.plot(oeis_id, (t2 - t_current) / 3600.0, '.', markersize = 0.5)

    plt.subplot(334)
    plt.hist(age / 3600.0, bins = 200, log = True)
    plt.xlabel("age [h]")

    plt.subplot(335)
    plt.xlabel("stability [h]")
    plt.ylabel("age [h]")
    plt.plot(stability / 3600.0, age / 3600.0, '.', markersize = 0.5)

    plt.subplot(336)
    #bins = np.logspace(-10.0, +20.0, 200)
    #plt.hist(np.log10(priority), bins = bins, log = True)

    RANGE_MAX = 0.15

    plt.hist(priority, range = (0, RANGE_MAX), bins = 200, log = True)

    plt.xlabel("priority ({} values > {:.3f})".format(np.sum(priority > RANGE_MAX), RANGE_MAX))

    plt.subplot(338)
    plt.hist(stability / 3600.0, bins = 200, log = True)
    plt.xlabel("stability [h]")

    plt.show()

def main():

    if len(sys.argv) != 2:
        print("Please specify the name of an OEIS database in Sqlite3 format.")
        return

    database_filename = sys.argv[1]

    with setup_logging(None):
        show_entries(database_filename)

if __name__ == "__main__":
    main()
