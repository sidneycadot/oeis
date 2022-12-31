#! /usr/bin/env python3

import os
import time
import sqlite3
import datetime
import logging
import argparse

import numpy as np
from matplotlib import pyplot as plt

from utilities.exit_scope import close_when_done
from utilities.setup_logging import setup_logging

logger = logging.getLogger(__name__)


def show_entries(database_filename: str) -> None:
    """Read database entries and show a plot of their timing information."""

    if not os.path.exists(database_filename):
        logger.critical("Database file '%s' not found! Unable to continue.", database_filename)
        return

    t_now = time.time()

    with close_when_done(sqlite3.connect(database_filename)) as db_conn, close_when_done(db_conn.cursor()) as dbcursor:
        query = "SELECT oeis_id, t1, t2 FROM oeis_entries;"
        dbcursor.execute(query)
        data = dbcursor.fetchall()

    data = np.array(data, dtype = [
            ("oeis_id", np.int32),
            ("t1", np.float64),
            ("t2", np.float64)
        ])

    t1 = data["t1"]
    t2 = data["t2"]
    oeis_id = data["oeis_id"]

    age = t_now - t2
    stability = t2 - t1

    priority = age / np.maximum(stability, 1e-6)

    plt.clf()

    dt_now = datetime.datetime.fromtimestamp(t_now)
    dt_now_str = dt_now.strftime("%Y-%m-%d %H:%M:%S")

    plt.suptitle("OEIS local database status on {}\n{} entries; oldest: {:.1f} min; youngest: {:.1f} min".format(
        dt_now_str, len(data), np.amax(age) / 60.0, np.amin(age) / 60.0))

    plt.subplots_adjust(wspace = 0.6, hspace=0.6)

    plt.subplot(331)
    plt.xlabel("oeis id")
    plt.ylabel("t1 [h]")
    plt.plot(oeis_id, (t1 - t_now) / 3600.0, '.', markersize = 0.5)

    plt.subplot(332)
    plt.xlabel("oeis id")
    plt.ylabel("t2 [h]")
    plt.plot(oeis_id, (t2 - t_now) / 3600.0, '.', markersize = 0.5)

    plt.subplot(334)
    plt.hist(age / 3600.0, bins = 200, log = True)
    plt.xlabel("age [h]")

    plt.subplot(335)
    plt.xlabel("stability [h]")
    plt.ylabel("age [h]")
    plt.plot(stability / 3600.0, age / 3600.0, '.', markersize = 0.5)

    plt.subplot(336)

    (almost_highest, really_highest) = np.percentile(priority, [99.999, 100.0])
    range_max = really_highest if really_highest / almost_highest < 2.0 else almost_highest

    plt.hist(priority, range = (0, range_max), bins = 200, log = True)

    left_out = np.sum(priority > range_max)
    if left_out == 0:
        plt.xlabel("priority (age/stability) [-]")
    else:
        plt.xlabel("priority (age/stability) [-]\n(omitted highest {} entries)".format(left_out))

    plt.subplot(338)
    plt.hist(stability / 3600.0, bins = 200, log = True)
    plt.xlabel("stability [h]")

    plt.show()


def main():

    parser = argparse.ArgumentParser(description="Show graphs of timing info of OEIS entries in an SQLite3 database.")

    parser.add_argument("filename", type=str, default="oeis.sqlite3")

    args = parser.parse_args()

    with setup_logging(None):
        logging.getLogger("matplotlib").setLevel(logging.WARNING)
        show_entries(args.filename)


if __name__ == "__main__":
    main()
