#! /usr/bin/env python3

import time
import sqlite3
import random
import multiprocessing
import logging
import lzma

from timer import start_timer
from fetch_remote_oeis_entry import fetch_remote_oeis_entry, BadOeisResponse

logger = logging.getLogger(__name__)

def ensure_database_schema_created(dbconn):

    schema = """
             CREATE TABLE IF NOT EXISTS oeis_entries (
                 oeis_id       INTEGER  PRIMARY KEY NOT NULL, -- OEIS ID number.
                 t1            REAL                 NOT NULL, -- earliest timestamp when the content below was first fetched.
                 t2            REAL                 NOT NULL, -- most recent timestamp when the content below was fetched.
                 main_content  TEXT                 NOT NULL, -- main content (i.e., lines starting with '%' sign).
                 bfile_content TEXT                 NOT NULL  -- b-file content (secondary file containing sequence entries).
             );
             """

    schema = "\n".join(line[13:] for line in schema.split("\n"))[1:-1]
    dbconn.execute(schema)

def find_highest_oeis_id():

    SLEEP_AFTER_FAILURE = 5.0

    success_id =  262000 # we know a-priori that this entry exists
    failure_id = 1000000 # we know a-priori that this entry does not exist

    # Do a binary search, looking for the success/failure boundary.
    while success_id + 1 != failure_id:

        fetch_id = (success_id + failure_id) // 2

        logger.info("OEIS search range is ({}, {}), attempting to fetch entry {} ...".format(success_id, failure_id, fetch_id))

        try:
            fetch_remote_oeis_entry(fetch_id, fetch_bfile_flag = False)
        except BadOeisResponse:
            # This exception happens when trying to read beyond the last entry in the database.
            # We mark the failure and continue the binary search.
            logging.info("OEIS entry {} does not exist.".format(fetch_id))
            failure_id = fetch_id
        except BaseException as exception:
            # Some other error occurred. We have to retry.
            logger.error("Unexpected fetch result ({}), retrying in {} seconds.".format(exception, SLEEP_AFTER_FAILURE))
            time.sleep(SLEEP_AFTER_FAILURE)
        else:
            # We mark the success and continue the binary search.
            logging.info("OEIS entry {} exists.".format(fetch_id))
            success_id = fetch_id

    logger.info("Last valid OEIS entry is A{:06}.".format(success_id))

    return success_id

def safe_fetch_remote_oeis_entry(entry):
    # Intercepts and reports any exceptions.
    # The (parallel or sequential) map must run to completion.
    try:
        result = fetch_remote_oeis_entry(entry, True)
    except BaseException as exception:
        logger.error("Unable to fetch entry {}: '{}'.".format(entry, exception))
        result = None
    return result

def fetch_entries_into_database(dbconn, entries):

    FETCH_BATCH_SIZE  =  200 # 100 -- 1000 are reasonable
    NUM_PROCESSES     =   10 # 10 -- 20 are reasonable
    SLEEP_AFTER_BATCH =  2.0 # [seconds]

    entries = set(entries)

    if NUM_PROCESSES > 1:
        pool = multiprocessing.Pool(NUM_PROCESSES)
    else:
        pool = None

    try:

        tStart = time.time()
        nStart = len(entries)

        while len(entries) > 0:

            random_entries_count = min(FETCH_BATCH_SIZE, len(entries))
            random_entries_to_be_fetched = random.sample(entries, random_entries_count)

            logger.info("Fetching data using {} {} for {} out of {} entries ...".format(NUM_PROCESSES, "worker" if NUM_PROCESSES == 1 else "workers", len(random_entries_to_be_fetched), len(entries)))

            t1 = time.time()

            if pool is None:
                # execute fetches sequentially
                results = [safe_fetch_remote_oeis_entry(entry) for entry in random_entries_to_be_fetched]
            else:
                # execute fetches in parallel
                results = pool.map(safe_fetch_remote_oeis_entry, random_entries_to_be_fetched)

            t2 = time.time()

            duration = (t2 - t1)
            logger.info("{} fetches took {:.3f} seconds ({:.3f} fetches/second).".format(len(random_entries_to_be_fetched), duration, len(random_entries_to_be_fetched) / duration))

            # Process the fetch results.

            countFailures         = 0
            countNewEntries       = 0
            countIdenticalEntries = 0
            countUpdatedEntries   = 0

            for result in results:

                if result is None:
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
                        query = "INSERT INTO oeis_entries(oeis_id, t1, t2, main_content, bfile_content) VALUES (?, ?, ?, ?, ?);"
                        dbcursor.execute(query, (result.oeis_id, result.timestamp, result.timestamp, result.main_content, result.bfile_content))
                        countNewEntries += 1
                    elif previous_content != (result.main_content, result.bfile_content):
                        # The database content is stale.
                        # Update t1, t2, and content.
                        query = "UPDATE oeis_entries SET t1 = ?, t2 = ?, main_content = ?, bfile_content = ? WHERE oeis_id = ?;"
                        dbcursor.execute(query, (result.timestamp, result.timestamp, result.main_content, result.bfile_content, result.oeis_id))
                        countUpdatedEntries += 1
                    else:
                        # The database content is identical to the freshly fetched content.
                        # We will just update the t2 field, indicating the fresh fetch.
                        query = "UPDATE oeis_entries SET t2 = ? WHERE oeis_id = ?;"
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
        if pool is not None:
            pool.close()
            pool.join()

def make_database_complete(dbconn, highest_oeis_id):

    dbcursor = dbconn.cursor()
    try:
        dbcursor.execute("SELECT oeis_id FROM oeis_entries;")
        present_entries = dbcursor.fetchall()
    finally:
        dbcursor.close()

    present_entries = [oeis_id for (oeis_id, ) in present_entries]
    logger.info("Entries present in local database: {}.".format(len(present_entries)))

    all_entries = range(1, highest_oeis_id + 1)

    missing_entries = set(all_entries) - set(present_entries)
    logger.info("Missing entries to be fetched: {}.".format(len(missing_entries)))

    fetch_entries_into_database(dbconn, missing_entries)

def update_database_entries_for_nonzero_time_window(dbconn):

    dbcursor = dbconn.cursor()
    try:
        dbcursor.execute("SELECT oeis_id FROM oeis_entries WHERE t1 = t2;")
        zero_timewindow_entries = dbcursor.fetchall()
    finally:
        dbcursor.close()

    zero_timewindow_entries = [oeis_id for (oeis_id, ) in zero_timewindow_entries]

    logger.info("Entries with zero time window in local database selected for refresh: {}.".format(len(zero_timewindow_entries)))

    fetch_entries_into_database(dbconn, zero_timewindow_entries)

def update_database_entries_randomly(dbconn, howmany):

    dbcursor = dbconn.cursor()
    try:
        dbcursor.execute("SELECT oeis_id FROM oeis_entries;")
        present_entries = dbcursor.fetchall()
    finally:
        dbcursor.close()

    present_entries = [oeis_id for (oeis_id, ) in present_entries]

    random_entries_count = min(howmany, len(present_entries))

    random_entries = random.sample(present_entries, random_entries_count)

    logger.info("Random entries in local database selected for refresh: {}.".format(len(random_entries)))

    fetch_entries_into_database(dbconn, random_entries)

def update_database_entries_by_score(dbconn, howmany):

    t_current = time.time()

    dbcursor = dbconn.cursor()
    try:
        query = "SELECT oeis_id FROM oeis_entries ORDER BY (? - t2) / max(t2 - t1, 1e-6) DESC LIMIT ?;"
        dbcursor.execute(query, (t_current, howmany))
        highest_score_entries = dbcursor.fetchall()
    finally:
        dbcursor.close()

    highest_score_entries = [oeis_id for (oeis_id, ) in highest_score_entries]
    logger.info("Highest-score entries in local database selected for refresh: {}.".format(len(highest_score_entries)))

    fetch_entries_into_database(dbconn, highest_score_entries)

def vacuum_database(dbconn):
    with start_timer() as timer:
        logger.info("Initiating VACUUM on database ...")
        dbconn.execute("VACUUM;")
        logger.info("VACUUM done in {}.".format(timer.duration_string()))

def compress_file(from_filename, to_filename):

    PRESET = 9 # level 9 without 'extra' works best on our data.

    with start_timer() as timer, open(from_filename, "rb") as f:
        logger.info("Reading uncompressed data from '{}' ...".format(from_filename))
        data = f.read()
        logger.info("Reading uncompressed data took {}.".format(timer.duration_string()))

    with start_timer() as timer, lzma.open(to_filename, "wb", format = lzma.FORMAT_XZ, check = lzma.CHECK_CRC64, preset = PRESET) as f:
        logger.info("Writing compressed data to '{}' ...".format(to_filename))
        f.write(data)
        logger.info("Writing compressed data took {}.".format(timer.duration_string()))

def consolidate_database_daily(database_filename):

    xz_filename = datetime.datetime.now().strftime("oeis_v%Y%m%d.sqlite3.xz")
    if os.path.exists(xz_filename):
        return # file already exists.

    with start_timer() as timer:

        # Vacuum the database
        dbconn = sqlite3.connect(database_filename)
        try:
            vacuum_database(dbconn)
        finally:
            dbconn.close()

        # Create the xz file.
        compress_file(database_filename, xz_filename)

        # Remove stale files.
        stale_files = [filename for filename in glob.glob("oeis_v????????.sqlite3.xz") if filename != xz_filename]
        for filename in stale_files:
            log.info("Removing stale consolidated file '{}' ...", 
            os.remove(filename)

        logger.info("Consolidating data took {}.".format(timer.duration_string()))

def database_update_cycle(database_filename):

    while True:

        # Perform an update cycle.

        with start_timer() as timer:

            dbconn = sqlite3.connect(database_filename)
            try:
                ensure_database_schema_created(dbconn)
                highest_oeis_id = find_highest_oeis_id()
                make_database_complete(dbconn, highest_oeis_id)                   # make sure we have all entries (full fetch on first run)
                update_database_entries_for_nonzero_time_window(dbconn)           # make sure we have t1 != t2 for all entries (full fetch on first run)
                update_database_entries_randomly(dbconn, highest_oeis_id // 1000) # refresh 0.1 % of entries randomly
                update_database_entries_by_score(dbconn, highest_oeis_id //  100) # refresh 1.0 % of entries by score
            finally:
                dbconn.close()

            consolidate_database_daily()

            logger.info("Full database update cycle took {}.".format(timer.duration_string()))

        # Pause between update cycles.

        PAUSE = max(300.0, random.gauss(1800.0, 600.0))
        logger.info("Sleeping for {:.1f} seconds ...".format(PAUSE))
        time.sleep(PAUSE)

def main():

    database_filename = "oeis.sqlite3"

    FORMAT = "%(asctime)-15s | %(levelname)-8s | %(message)s"
    logging.basicConfig(format = FORMAT, level = logging.DEBUG)

    try:
        database_update_cycle(database_filename)
    finally:
        logging.shutdown()

if __name__ == "__main__":
    main()
