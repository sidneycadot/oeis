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
        query = "SELECT t_first_fetch, t_most_recent FROM oeis_entries;"
        dbcursor.execute(query)
        data = dbcursor.fetchall()
    finally:
        dbcursor.close()

    dt = np.dtype([
            ("t1", np.float64),
            ("t2", np.float64)
        ]
    )

    data = np.array(data, dtype = dt)

    print(data.shape, data.dtype)

    age       = t_current - data["t2"]
    stability = data["t2"] - data["t1"]

    score = age / np.maximum(stability, 1e-6)

    plt.subplot(311)
    plt.xlabel("stability")
    plt.ylabel("age")
    plt.plot(stability, age, '.')
    plt.subplot(312)
    plt.hist(stability, bins = 200)
    plt.xlabel("stability")
    plt.subplot(313)
    plt.hist(score, bins = 200, range = (0, 0.10))
    plt.xlabel("score")
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
