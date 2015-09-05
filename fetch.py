#! /usr/bin/env python3

# As of Sept 5, 2015, the last value is:
#      http://oeis.org/A261892

#import os
import time
import sqlite3
import urllib.request
import random
import multiprocessing
import logging
from bs4 import BeautifulSoup, NavigableString

def fetch_oeis_internal_entry(oeis_id):

    url  = "http://oeis.org/A{:06d}/internal".format(oeis_id)

    try:
        timestamp = time.time()

        with urllib.request.urlopen(url) as response:
            fetch_html = response.read()

        soup = BeautifulSoup(fetch_html, 'html.parser')
        contents = []

        for tt in soup.find_all("tt"):

            contents_line = tt.contents

            if len(contents_line) == 0:
                # Empty 'tt' tags do occur.
                continue

            assert len(contents_line) == 1
            contents_line = contents_line[0]

            assert isinstance(contents_line, NavigableString)
            contents_line = str(contents_line)

            assert contents_line.endswith("\n")
            contents_line = contents_line[:-1]

            if contents_line.startswith("%"):
                contents.append(contents_line)

        contents = "\n".join(contents)

    except BaseException as exception:
        logging.error("error while fetching '{}'".format(url))
        return None

    return (oeis_id, timestamp, contents)

def main():

    FORMAT = "%(asctime)-15s | %(levelname)-10s | %(message)s"
    logging.basicConfig(format = FORMAT, level = logging.DEBUG)

    logger = logging.getLogger(__name__)

    database_filename = "fetch_oeis_internal.sqlite3"

    dbconn = sqlite3.connect(database_filename)
    dbcursor = dbconn.cursor()

    schema = """
             CREATE TABLE IF NOT EXISTS fetched_oeis_entries (
                 id         INTEGER PRIMARY KEY,
                 oeis_id    INTEGER NOT NULL,
                 timestamp  INTEGER NOT NULL,
                 contents   BLOB    NOT NULL
             );
             CREATE INDEX IF NOT EXISTS fetched_oeis_entries_index ON fetched_oeis_entries(oeis_id);
             """

    schema = "\n".join(line[13:] for line in schema.split("\n"))[1:-1]
    dbcursor.executescript(schema)

    LAST_OEIS_ID = 261892
    all_entries = set(range(1, LAST_OEIS_ID + 1))
    FETCH_BATCH_SIZE = 200
    SLEEP_AFTER_BATCH = 1.5
    NUM_PROCESSES = 20

    pool = multiprocessing.Pool(NUM_PROCESSES)
    while True:

        dbcursor.execute("SELECT DISTINCT oeis_id FROM fetched_oeis_entries;")

        entries_in_local_database = dbcursor.fetchall()
        entries_in_local_database = set(oeis_id for (oeis_id, ) in entries_in_local_database)

        logger.info("entries present in local database: {}".format(len(entries_in_local_database)))

        entries_to_be_fetched = all_entries - entries_in_local_database

        logger.info("total number of entries still to be fetched: {}".format(len(entries_to_be_fetched)))

        if len(entries_to_be_fetched) == 0:
            break

        # randomly select some entries

        fetch_count = min(FETCH_BATCH_SIZE, len(entries_to_be_fetched))
        fetch_entries = random.sample(entries_to_be_fetched, fetch_count)

        logger.info("fetching {} entries ...".format(len(fetch_entries)))

        t1 = time.time()
        results = pool.map(fetch_oeis_internal_entry, fetch_entries)
        t2 = time.time()

        duration = (t2 - t1)
        logger.info("{} parallel fetches took {:.3f} seconds ({:.3f} fetches/second).".format(len(fetch_entries), duration, len(fetch_entries) / duration))

        results = [result for result in results if result is not None]

        logger.info("{} total bytes fetched in {} valid entries.".format(sum(len(result[-1]) for result in results), len(results)))

        dbcursor.executemany("INSERT OR REPLACE INTO fetched_oeis_entries(oeis_id, timestamp, contents) VALUES (?, ?, ?);", results)
        dbconn.commit()

        logger.info("sleeping ...")
        time.sleep(SLEEP_AFTER_BATCH)

if __name__ == "__main__":
    main()
