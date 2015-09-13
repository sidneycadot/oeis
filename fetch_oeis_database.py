#! /usr/bin/env python3

import re
import time
import sqlite3
import urllib.request
import random
import multiprocessing
import logging

logger = logging.getLogger(__name__)

def oeis_internal_html_to_content(html):

    # Parse the OEIS main sequence page html.
    # We previously used BeautifulSoup4 for this, but using regular expressions
    # works equally well, and it's one less dependency.

    # The pattern uses non-greedy matching; we also allow newlines in the matched pattern.
    tt_pattern = re.compile("<tt>%.*?</tt>", re.DOTALL)

    tt_matches = tt_pattern.findall(html)

    content = []

    for tt in tt_matches:
        assert tt.startswith("<tt>")
        assert tt.endswith("</tt>")
        tt = tt[4:-5]
        tt = tt.strip()
        content.append(tt)

    content = "\n".join(content)
    return content

class FetchFailure:
    ok = False
    is404 = False

class FetchFailure404(FetchFailure):
    is404 = True

class FetchSuccess:
    ok = True
    def __init__(self, oeis_id, timestamp, main_content, bfile_content):
        self.oeis_id       = oeis_id
        self.timestamp     = timestamp
        self.main_content  = main_content
        self.bfile_content = bfile_content

def fetch_oeis_internal_entry(oeis_id, fetch_bfile_flag = True):

    # We fetch the "internal" version of the OEIS entry,
    # which is easy to parse and documented.

    main_url  = "http://oeis.org/A{oeis_id:06d}/internal".format(oeis_id = oeis_id)
    bfile_url = "http://oeis.org/A{oeis_id:06d}/b{oeis_id:06d}.txt".format(oeis_id = oeis_id) if fetch_bfile_flag else None

    urls = (main_url, bfile_url)

    timestamp = time.time()

    # Try to fetch the main page.
    # Any failure that leads to an exception leads to an abort.

    url_response_data = []
    for url in urls:
        if url is not None:
            try:
                with urllib.request.urlopen(url) as response:
                    response_data = response.read().decode(response.headers.get_content_charset())
            except urllib.error.HTTPError as exception:
                logger.error("Error while fetching '{}': {}.".format(url, exception))
                if exception.code == 404:
                    return FetchFailure404()
                else:
                    return FetchFailure()
            except urllib.error.URLError as exception:
                # Log the failure, and return None.
                logger.error("Error while fetching '{}': {}.".format(url, exception))
                return FetchFailure()
        else:
            response_data = None
        url_response_data.append(response_data)

    (main_content, bfile_content) = url_response_data

    main_content = oeis_internal_html_to_content(main_content)

    return FetchSuccess(oeis_id, timestamp, main_content, bfile_content)

def find_highest_oeis_id():

    OEIS_SUCCESS =  260000
    OEIS_FAILURE = 1000000

    while OEIS_SUCCESS + 1 != OEIS_FAILURE:

        OEIS_ATTEMPT = (OEIS_SUCCESS + OEIS_FAILURE) // 2

        logger.info("Highest OEIS range is ({}, {}), attempting to fetch entry # {} ...".format(OEIS_SUCCESS, OEIS_FAILURE, OEIS_ATTEMPT))

        result = fetch_oeis_internal_entry(OEIS_ATTEMPT, fetch_bfile_flag = False)

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
                 oeis_id       INTEGER  PRIMARY KEY NOT NULL, -- OEIS ID number.
                 t_first_fetch REAL                 NOT NULL, -- earliest timestamp when the content below was first fetched.
                 t_most_recent REAL                 NOT NULL, -- most recent timestamp when the content below was fetched.
                 main_content  TEXT                 NOT NULL, -- main content (i.e., lines starting with '%' sign).
                 bfile_content TEXT                 NOT NULL  -- b-file content (secondary file containing sequence entries).
             );
             CREATE INDEX IF NOT EXISTS oeis_entries_index ON oeis_entries(oeis_id);
             """

    schema = "\n".join(line[13:] for line in schema.split("\n"))[1:-1]
    dbconn.executescript(schema)

def fetch_entries_into_database(dbconn, entries):

    assert isinstance(entries, set)

    FETCH_BATCH_SIZE  = 200 # 500
    NUM_PROCESSES     =  20 # 20
    SLEEP_AFTER_BATCH = 5.0 # [seconds]

    pool = multiprocessing.Pool(NUM_PROCESSES)
    try:

        tStart = time.time()
        nStart = len(entries)

        while len(entries) > 0:

            random_entries_count = min(FETCH_BATCH_SIZE, len(entries))
            random_entries_to_be_fetched = random.sample(entries, random_entries_count)

            logger.info("Executing parallel fetch for {} out of {} entries.".format(len(random_entries_to_be_fetched), len(entries)))

            t1 = time.time()
            results = pool.map(fetch_oeis_internal_entry, random_entries_to_be_fetched)
            #results = list(map(lambda entry : fetch_oeis_internal_entry(entry, fetch_bfile_flag = True), random_entries_to_be_fetched))

            t2 = time.time()

            duration = (t2 - t1)
            logger.info("{} parallel fetches took {:.3f} seconds ({:.3f} fetches/second).".format(len(random_entries_to_be_fetched), duration, len(random_entries_to_be_fetched) / duration))

            # Process the fetch results.

            countFailures         = 0
            countNewEntries       = 0
            countIdenticalEntries = 0
            countUpdatedEntries   = 0

            for result in results:

                if not result.ok:
                    # Skip entries that are not okay
                    countFailures += 1
                    continue

                dbcursor = dbconn.cursor()
                try:
                    query = "SELECT main_content, bfile_content FROM oeis_entries WHERE oeis_id = ?;"
                    dbcursor.execute(query, (result.oeis_id, ))

                    previous_content = dbcursor.fetchall()

                    assert len(previous_content) <= 1
                    previous_content = None if len(previous_content) == 0 else previous_content[0]

                    if previous_content is None:
                        # The oeis_id does not occur in the database yet. We will insert it.
                        query = "INSERT INTO oeis_entries(oeis_id, t_first_fetch, t_most_recent, main_content, bfile_content) VALUES (?, ?, ?, ?, ?);"
                        dbcursor.execute(query, (result.oeis_id, result.timestamp, result.timestamp, result.main_content, result.bfile_content))
                        countNewEntries += 1
                    elif previous_content != (result.main_content, result.bfile_content):
                        # The database content is stale.
                        # Update t_first_fetch, t_most_recent, and content.
                        query = "UPDATE oeis_entries SET t_first_fetch = ?, t_most_recent = ?, main_content = ?, bfile_content = ? WHERE oeis_id = ?;"
                        dbcursor.execute(query, (result.timestamp, result.timestamp, result.main_content, result.bfile_content, result.oeis_id))
                        countUpdatedEntries += 1
                    else:
                        # The database content is identical to the freshly fetched content.
                        # We will just update the t_most_recent field, indicating the fresh fetch.
                        query = "UPDATE oeis_entries SET t_most_recent = ? WHERE oeis_id = ?;"
                        dbcursor.execute(query, (result.timestamp, result.oeis_id))
                        countIdenticalEntries += 1
                finally:
                    dbcursor.close()

                # Mark entry as processed by removing it from the 'entries' set.
                entries.remove(result.oeis_id)

            dbconn.commit()

            logger.info("Processed {} results (failures: {}, new: {}, identical: {}, updated: {}).".format(len(results), countFailures, countNewEntries, countIdenticalEntries, countUpdatedEntries))

            # Calculate and show estimated-time-to-completion.

            tCurrent = time.time()
            nCurrent = len(entries)

            ETC = (tCurrent - tStart) * nCurrent / (nStart - nCurrent)
            logger.info("Estimated time to completion: {:.1f} minutes.".format(ETC / 60.0))

            # Sleep if we need to fetch more
            if len(entries) > 0:
                logger.info("Sleeping for {:.1f} seconds ...".format(SLEEP_AFTER_BATCH))
                time.sleep(SLEEP_AFTER_BATCH)

    finally:
        pool.close()
        pool.join()

def make_database_complete(dbconn, highest_oeis_id):

    dbcursor = dbconn.cursor()
    try:
        dbcursor.execute("SELECT oeis_id FROM oeis_entries;")
        entries_in_local_database = dbcursor.fetchall()
    finally:
        dbcursor.close()

    entries_in_local_database = set(oeis_id for (oeis_id, ) in entries_in_local_database)
    logger.info("Entries present in local database: {}.".format(len(entries_in_local_database)))

    all_entries = set(range(1, highest_oeis_id + 1))

    missing_entries = set(all_entries) - entries_in_local_database
    logger.info("Missing entries to be fetched: {}.".format(len(missing_entries)))

    fetch_entries_into_database(dbconn, missing_entries)

def update_database_entries_randomly(dbconn, howmany):

    dbcursor = dbconn.cursor()
    try:
        dbcursor.execute("SELECT oeis_id FROM oeis_entries;")
        all_entries = dbcursor.fetchall()
    finally:
        dbcursor.close()

    all_entries = [oeis_id for (oeis_id, ) in all_entries]

    random_entries_count = min(howmany, len(all_entries))
    random_entries = set(random.sample(all_entries, random_entries_count))

    logger.info("Random entries in local database selected for refresh: {}.".format(len(random_entries)))

    fetch_entries_into_database(dbconn, random_entries)

def update_database_entries_by_score(dbconn, howmany):

    t_current = time.time()

    dbcursor = dbconn.cursor()
    try:
        query = "SELECT oeis_id FROM oeis_entries ORDER BY (? - t_most_recent) / max(t_most_recent - t_first_fetch, 1e-6) DESC LIMIT ?;"
        dbcursor.execute(query, (t_current, howmany))
        highest_score_entries = dbcursor.fetchall()
    finally:
        dbcursor.close()

    highest_score_entries = set(oeis_id for (oeis_id, ) in highest_score_entries)
    logger.info("Highest-score entries in local database selected for refresh: {}.".format(len(highest_score_entries)))

    fetch_entries_into_database(dbconn, highest_score_entries)

def vacuum_database(dbconn):

    logger.info("Initiating VACUUM on database ...")

    t1 = time.time()
    dbconn.execute("VACUUM;")
    t2 = time.time()

    duration = (t2 - t1)

    logger.info("VACUUM done in {:.3f} seconds.".format(duration))

def main():

    FORMAT = "%(asctime)-15s | %(levelname)-10s | %(message)s"
    logging.basicConfig(format = FORMAT, level = logging.DEBUG)

    database_filename = "oeis.sqlite3"

    try:

        while True:
            dbconn = sqlite3.connect(database_filename)
            try:
                setup_schema(dbconn)
                highest_oeis_id = find_highest_oeis_id()
                make_database_complete(dbconn, highest_oeis_id)
                update_database_entries_randomly(dbconn, highest_oeis_id // 1000) # refresh 0.1% of entries randomly
                update_database_entries_by_score(dbconn, highest_oeis_id //  200) # refresh 0.5% of entries by score
                vacuum_database(dbconn)
            finally:
                dbconn.close()

            PAUSE = max(300.0, random.gauss(1800.0, 600.0))

            logger.info("Sleeping for {:.1f} seconds ...".format(PAUSE))
            time.sleep(PAUSE)

    finally:
        logging.shutdown()

if __name__ == "__main__":
    main()
