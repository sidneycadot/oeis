#! /usr/bin/env python3

import sqlite3
import collections
import numpy as np
import time
import re
import logging

def main():

    FORMAT = "%(asctime)-15s | %(levelname)-10s | %(message)s"
    logging.basicConfig(format = FORMAT, level = logging.DEBUG)

    logger = logging.getLogger(__name__)

    database_filename = "fetch_oeis_internal.sqlite3"

    dbconn = sqlite3.connect(database_filename)
    dbcursor = dbconn.cursor()

    t1 = time.time()
    dbcursor.execute("SELECT oeis_id, contents FROM fetched_oeis_entries;")
    data = dbcursor.fetchall()
    t2 = time.time()
    duration = (t2 - t1)
    logger.info("Full fetch from database: {} entries in {:.3f} seconds".format(len(data), duration))

    t1 = time.time()
    data.sort() # sort by OEIS ID
    t2 = time.time()
    duration = (t2 - t1)
    logger.info("Sort by OEIS-ID took {:.3f} seconds.".format(duration))

if __name__ == "__main__":
    main()
