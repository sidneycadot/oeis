#! /usr/bin/env python3

import os
import time
import sqlite3
import datetime
import logging
import argparse
from typing import Optional

import numpy as np
from matplotlib import pyplot as plt

from utilities.exit_scope import close_when_done
from utilities.setup_logging import setup_logging

logger = logging.getLogger(__name__)


def show_entries(database_filename: str, output_filename: Optional[str], output_dpi: Optional[int]) -> None:
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

    plt.gcf().set_size_inches(16, 9)

    dt_now = datetime.datetime.fromtimestamp(t_now)
    dt_now_str = dt_now.strftime("%Y-%m-%d %H:%M:%S")

    plt.suptitle("OEIS local database status on {}:\n{} entries with ages from {:.1f} minutes to {:.3f} days.".format(
        dt_now_str, len(data), np.amin(age) / 60.0, np.amax(age) / 86400.0))

    plt.subplots_adjust(wspace = 0.6, hspace=0.6)

    plt.subplot(331)
    plt.xlabel("oeis id (×1000)")
    plt.ylabel("t1 [h]")
    plt.grid()
    plt.plot(oeis_id / 1000.0, (t1 - t_now) / 3600.0, '.', markersize = 0.5)

    plt.subplot(332)
    plt.xlabel("oeis id (×1000)")
    plt.ylabel("t2 [h]")
    plt.grid()
    plt.plot(oeis_id / 1000.0, (t2 - t_now) / 3600.0, '.', markersize = 0.5)

    plt.subplot(333)
    plt.xlabel("t1 [h]")
    plt.ylabel("t2 [h]")
    plt.grid()
    plt.plot((t1 - t_now) / 3600.0, (t2 - t_now) / 3600.0, '.', markersize = 0.5)

    plt.subplot(334)
    plt.hist(age / 3600.0, bins = 200, log = True)
    plt.grid()
    plt.xlabel("age [h]")

    plt.subplot(335)
    plt.xlabel("stability [h]")
    plt.ylabel("age [h]")
    plt.grid()
    plt.plot(stability / 3600.0, age / 3600.0, '.', markersize = 0.5)

    plt.subplot(336)

    (almost_highest, really_highest) = np.percentile(priority, [99.99, 100.0])
    range_max = really_highest if really_highest / almost_highest < 2.0 else almost_highest

    plt.grid()
    plt.hist(priority, range = (0, range_max), bins = 200, log = True)

    left_out = np.sum(priority > range_max)
    if left_out == 0:
        plt.xlabel("priority (age/stability) [-]")
    else:
        plt.xlabel("priority (age/stability) [-]\n(omitted highest {} entries)".format(left_out))

    plt.subplot(338)
    plt.xlabel("stability [h]")
    plt.grid()
    plt.hist(stability / 3600.0, bins = 200, log = True)

    if output_filename is None:
        plt.show()
    else:
        plt.savefig(output_filename, dpi=output_dpi)


def main():

    default_database_filename = "oeis.sqlite3"

    parser = argparse.ArgumentParser(description="Show graphs of timing info of OEIS entries in an SQLite3 database.")

    parser.add_argument("-f", dest="database_filename", type=str, default=default_database_filename, help="OEIS SQLite3 database (default: {})".format(default_database_filename))
    parser.add_argument("-o", dest="output_filename", type=str, help="Output filename")
    parser.add_argument("--dpi", dest="output_dpi", type=int, help="Output file dots-per-inch")

    args = parser.parse_args()

    with setup_logging():
        logging.getLogger("matplotlib").setLevel(logging.WARNING)
        show_entries(args.database_filename, args.output_filename, args.output_dpi)


if __name__ == "__main__":
    main()
