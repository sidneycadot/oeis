#! /usr/bin/env python3

# As of Sept 5, 2015, the last value is:
#      http://oeis.org/A261887

import os
import time
import sqlite3
import urllib.request
import platform
import random
import multiprocessing
import logging

FORMAT = '%(asctime)-15s | %(message)s'
logging.basicConfig(format = FORMAT, level = logging.DEBUG)

logger = logging.getLogger(__name__)

def fetch_oeis_entry(oeis_id):
    fetch_node = platform.node()
    fetch_url  = "http://oeis.org/A{:06d}".format(oeis_id)
    try:
        t1 = time.time()
        with urllib.request.urlopen(fetch_url) as response:
            fetch_html = response.read()
        t2 = time.time()
        fetch_timestamp = t1
        fetch_duration = (t2 - t1)
    except BaseException as exception:
        logging.exception("error while fetching '{}'".format(fetch_url))
        return None
    return (oeis_id, fetch_node, fetch_url, fetch_timestamp, fetch_duration, fetch_html)

database_filename = "fetch_oeis.sqlite3"

dbconn = sqlite3.connect(database_filename)
dbcursor = dbconn.cursor()

schema = """
         CREATE TABLE IF NOT EXISTS fetched_oeis_entries (
             id               INTEGER PRIMARY KEY,
             oeis_id          INTEGER NOT NULL,
             fetch_node       TEXT    NOT NULL,
             fetch_url        TEXT    NOT NULL,
             fetch_timestamp  INTEGER NOT NULL,
             fetch_duration   REAL    NOT NULL,
             fetch_html       BLOB    NOT NULL
         );
         CREATE INDEX IF NOT EXISTS oeis_id_index ON fetched_oeis_entries(oeis_id);
         """

schema = "\n".join(line[9:] for line in schema.split("\n"))[1:-1]
dbcursor.executescript(schema)

LAST_OEIS_ID = 261887
WANTED_ENTRIES = set(range(1, LAST_OEIS_ID + 1))
FETCH_BATCH_SIZE = 100
SLEEP_AFTER_BATCH = 1
NUM_PROCESSES = 20

pool = multiprocessing.Pool(NUM_PROCESSES)
while True:

    dbcursor.execute("SELECT DISTINCT oeis_id FROM fetched_oeis_entries;")
    entries_in_local_database = dbcursor.fetchall()
    entries_in_local_database = set(oeis_id for (oeis_id, ) in entries_in_local_database)

    logger.info("entries present in local database: {}".format(len(entries_in_local_database)))

    NEEDED_ENTRIES = WANTED_ENTRIES - entries_in_local_database

    logger.info("needed entries: {}".format(len(NEEDED_ENTRIES)))

    fetch_count = min(FETCH_BATCH_SIZE, len(NEEDED_ENTRIES))
    FETCH_ENTRIES = random.sample(NEEDED_ENTRIES, fetch_count)

    logger.info("fetching {} entries ...".format(len(FETCH_ENTRIES)))

    t1 = time.time()
    results = pool.map(fetch_oeis_entry, FETCH_ENTRIES)
    t2 = time.time()

    duration = (t2 - t1)
    logger.info("{} parallel fetches took {:.3f} seconds ({:.3f} fetches/second).".format(len(FETCH_ENTRIES), duration, len(FETCH_ENTRIES) / duration))

    results = [result for result in results if result is not None]

    logger.info("{} fetched entries are valid.".format(len(FETCH_ENTRIES)))

    dbcursor.executemany("INSERT INTO fetched_oeis_entries(oeis_id, fetch_node, fetch_url, fetch_timestamp, fetch_duration, fetch_html) VALUES (?, ?, ?, ?, ?, ?);", results)
    dbconn.commit()

    logger.info("sleeping...")
    time.sleep(SLEEP_AFTER_BATCH)
