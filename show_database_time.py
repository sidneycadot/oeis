#! /usr/bin/env python3

import time
import logging
import sqlite3
import numpy as np
from matplotlib import pyplot as plt

logger = logging.getLogger(__name__)

def show_entries(dbconn):

    t_current = time.time()

    dbcursor = dbconn.cursor()
    try:
        query = "SELECT oeis_id, t_first_fetch, t_most_recent FROM oeis_entries;"
        dbcursor.execute(query)
        data = dbcursor.fetchall()
    finally:
        dbcursor.close()

    dt = np.dtype([
            ("oeis_id", np.int),
            ("t1"     , np.float64),
            ("t2"     , np.float64)
        ]
    )

    data = np.array(data, dtype = dt)

    print(data.shape, data.dtype)

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
    MAXRANGE = 1.0
    plt.hist(score, bins = 200, log = True, range = (0, 1.0))
    plt.xlabel("score [-] ({} entries > {})".format(np.sum(score > MAXRANGE), MAXRANGE))

    plt.subplot(338)
    plt.hist(stability / 3600.0, bins = 200, log = True)
    plt.xlabel("stability [h]")

    plt.show()

def main():

    FORMAT = "%(asctime)-15s | %(levelname)-10s | %(message)s"
    logging.basicConfig(format = FORMAT, level = logging.DEBUG)

    database_filename = "oeis.sqlite3"

    dbconn = sqlite3.connect(database_filename)
    try:
        show_entries(dbconn)
    finally:
        dbconn.close()

    logging.shutdown()

if __name__ == "__main__":
    main()
