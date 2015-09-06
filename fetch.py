#! /usr/bin/env python3

import time
import sqlite3
import urllib.request
import random
import multiprocessing
import logging

from bs4 import BeautifulSoup, NavigableString

logger = logging.getLogger(__name__)

def oeis_internal_html_to_contents(html):
    """ Parse the OEIS html
    """
    soup = BeautifulSoup(html, 'html.parser')
    contents = []

    for tt in soup.find_all("tt"):

        contents_line = tt.contents

        if len(contents_line) == 0:
            # Empty 'tt' tags do occur; we discard them.
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

    return contents

class FetchFailure:
    ok = False
    is404 = False

class FetchFailure404(FetchFailure):
    is404 = True

class FetchSuccess:
    ok = True
    def __init__(self, oeis_id, timestamp, contents):
        self.oeis_id = oeis_id
        self.timestamp = timestamp
        self.contents = contents

def fetch_oeis_internal_entry(oeis_id):

    # We fetch the "internal" version of the OEIS entry,
    # which is easy to parse and documented.

    url = "http://oeis.org/A{:06d}/internal".format(oeis_id)

    timestamp = time.time()

    # Try to fetch the page.
    # Any failure that leads to an exception leads to an abort.

    try:
        with urllib.request.urlopen(url) as response:
            html = response.read()
    except urllib.error.HTTPError as exception:
        logger.error("Error while fetching '{}': {}".format(url, exception))
        if exception.code == 404:
            return FetchFailure404()
        else:
            return FetchFailure()
    except urllib.error.URLError as exception:
        # Log the failure, and return None.
        logger.error("Error while fetching '{}': {}".format(url, exception))
        return FetchFailure()

    contents = oeis_internal_html_to_contents(html)

    return FetchSuccess(oeis_id, timestamp, contents)

def find_highest_oeis_id():

    OEIS_SUCCESS =  260000
    OEIS_FAILURE = 1000000

    while OEIS_SUCCESS + 1 != OEIS_FAILURE:

        OEIS_ATTEMPT = (OEIS_SUCCESS + OEIS_FAILURE) // 2

        logger.info("Highest OEIS range is ({}, {}), attempting to fetch entry # {} ...".format(OEIS_SUCCESS, OEIS_FAILURE, OEIS_ATTEMPT))

        result = fetch_oeis_internal_entry(OEIS_ATTEMPT)

        if result.ok: # success!
            OEIS_SUCCESS = OEIS_ATTEMPT
        elif result.is404:
            # Pages that don't exist are indicated with a HTTP 404 response.
            OEIS_FAILURE = OEIS_ATTEMPT
        else:
            # Some other error occurred. We have to retry.
            logger.warning("Unexpected fetch result, retrying in 5 seconds.")
            time.sleep(5.0)

    logger.info("Highest OEIS entry is A{:06}.".format(OEIS_SUCCESS))
    return OEIS_SUCCESS

def setup_schema(dbconn):

    schema = """
             CREATE TABLE IF NOT EXISTS oeis_entries (
                 oeis_id    INTEGER  PRIMARY KEY NOT NULL,
                 timestamp  REAL                 NOT NULL,
                 contents   TEXT                 NOT NULL
             )
             """

    schema = "\n".join(line[13:] for line in schema.split("\n"))[1:-1]
    dbconn.executescript(schema)

def fetch_entries_into_database(dbconn, entries):

    FETCH_BATCH_SIZE  = 500
    NUM_PROCESSES     =  20
    SLEEP_AFTER_BATCH = 5.0 # [seconds]

    pool = multiprocessing.Pool(NUM_PROCESSES)

    tStart = time.time()
    nStart = len(entries)

    while len(entries) > 0:

        random_entries_count = min(FETCH_BATCH_SIZE, len(entries))
        random_entries_to_be_fetched = random.sample(entries, random_entries_count)

        logger.info("Executing parallel fetch for {} out of {} entries.".format(len(random_entries_to_be_fetched), len(entries)))

        t1 = time.time()
        results = pool.map(fetch_oeis_internal_entry, random_entries_to_be_fetched)
        t2 = time.time()

        duration = (t2 - t1)
        logger.info("{} parallel fetches took {:.3f} seconds ({:.3f} fetches/second).".format(len(random_entries_to_be_fetched), duration, len(random_entries_to_be_fetched) / duration))

        results = [result for result in results if result.ok]

        logger.info("Inserting {} valid entries ({} bytes) in database ...".format(len(results), sum(len(result.contents) for result in results)))

        insert_entries = [(result.oeis_id, result.timestamp, result.contents) for result in results]

        dbconn.executemany("INSERT OR REPLACE INTO oeis_entries(oeis_id, timestamp, contents) VALUES (?, ?, ?)", insert_entries)
        dbconn.commit()

        # Mark succesful entries as fetched

        entries -= set(result.oeis_id for result in results)

        tCurrent = time.time()
        nCurrent = len(entries)

        ETC = (tCurrent - tStart) * nCurrent / (nStart - nCurrent)
        logger.info("Estimated time to completion: {:.1f} minutes.".format(ETC / 60.0))

        # Sleep

        logger.info("Sleeping for {:.1f} seconds ...".format(SLEEP_AFTER_BATCH))
        time.sleep(SLEEP_AFTER_BATCH)

def make_database_complete(dbconn, highest_oeis_id):

    dbcursor = dbconn.cursor()
    try:
        dbcursor.execute("SELECT oeis_id FROM oeis_entries")
        entries_in_local_database = dbcursor.fetchall()
    finally:
        dbcursor.close()

    entries_in_local_database = set(oeis_id for (oeis_id, ) in entries_in_local_database)
    logger.info("Entries present in local database: {}".format(len(entries_in_local_database)))

    all_entries = set(range(1, highest_oeis_id + 1))

    missing_entries = set(all_entries) - entries_in_local_database
    logger.info("Missing entries to be fetched: {}".format(len(missing_entries)))

    fetch_entries_into_database(dbconn, missing_entries)

def refresh_oldest_database_entries(dbconn, howmany):

    dbcursor = dbconn.cursor()
    try:
        dbcursor.execute("SELECT oeis_id FROM oeis_entries ORDER BY timestamp LIMIT {}".format(howmany))
        oldest_entries = dbcursor.fetchall()
    finally:
        dbcursor.close()

    oldest_entries = set(oeis_id for (oeis_id, ) in oldest_entries)
    logger.info("Oldest entries in local database selected for refresh: {}".format(len(oldest_entries)))

    fetch_entries_into_database(dbconn, oldest_entries)

def vacuum_database(dbconn):

    logger.info("Initiating VACUUM on database ...")

    t1 = time.time()
    dbconn.execute("VACUUM")
    t2 = time.time()

    duration = (t2 - t1)

    logger.info("VACUUM done in {:.3f} seconds.".format(duration))

def main():

    FORMAT = "%(asctime)-15s | %(levelname)-10s | %(message)s"
    logging.basicConfig(format = FORMAT, level = logging.DEBUG)

    database_filename = "oeis.sqlite3"

    while True:

        dbconn = sqlite3.connect(database_filename)
        try:
            setup_schema(dbconn)
            highest_oeis_id = find_highest_oeis_id()
            make_database_complete(dbconn, highest_oeis_id)
            refresh_oldest_database_entries(dbconn, highest_oeis_id // 100) # refresh 1% of entries
            vacuum_database(dbconn)
        finally:
            dbconn.close()

        logger.info("Sleeping for 1 hour ...")
        time.sleep(3600)

    logging.shutdown()

if __name__ == "__main__":
    main()
