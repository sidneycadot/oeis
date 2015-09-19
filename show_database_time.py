#! /usr/bin/env python3

import os
import sys
import time
import logging
import sqlite3
import numpy as np
from matplotlib import pyplot as plt

logger = logging.getLogger(__name__)

def show_entries(database_filename):

    if not os.path.exists(database_filename):
        logger.critical("Database file '{}' not found! Unable to continue.".format(database_filename))
        return

    t_current = time.time()

    dbconn = sqlite3.connect(database_filename)
    try:
        dbcursor = dbconn.cursor()
        try:
            query = "SELECT oeis_id, t1, t2 FROM oeis_entries;"
            dbcursor.execute(query)
            data = dbcursor.fetchall()
        finally:
            dbcursor.close()
    finally:
        dbconn.close()

    dt = np.dtype([
            ("oeis_id", np.int),
            ("t1"     , np.float64),
            ("t2"     , np.float64)
        ]
    )

    data = np.array(data, dtype = dt)

    # print(data.shape, data.dtype)

    t1      = data["t1"]
    t2      = data["t2"]
    oeis_id = data["oeis_id"]

    age       = t_current - t2
    stability = t2 - t1

    score = age / np.maximum(stability, 1e-6)

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
    bins = np.logspace(-4.0, +2.0, 200)
    plt.hist(np.log10(score), bins = bins, log = True)
    plt.xscale("log")
    plt.xlabel("score")

    plt.subplot(338)
    plt.hist(stability / 3600.0, bins = 200, log = True)
    plt.xlabel("stability [h]")

    plt.show()

def main():

    if len(sys.argv) != 2:
        print("Please specify the name of an OEIS database in Sqlite3 format.")
        return

    database_filename = sys.argv[1]

    FORMAT = "%(asctime)-15s | %(levelname)-8s | %(message)s"
    logging.basicConfig(format = FORMAT, level = logging.DEBUG)

    try:
        show_entries(database_filename)
    finally:
        logging.shutdown()

if __name__ == "__main__":
    main()
