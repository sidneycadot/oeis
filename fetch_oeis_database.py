#! /usr/bin/env python3

import time
import sqlite3
import random
import multiprocessing
import logging

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

    assert isinstance(entries, set)

    FETCH_BATCH_SIZE  = 1000 # 200 -- 1000 are reasonable
    NUM_PROCESSES     =   20 # 20
    SLEEP_AFTER_BATCH =  2.0 # [seconds]

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

            logger.info("Executing fetch using {} {} for {} out of {} entries.".format(len(random_entries_to_be_fetched), NUM_PROCESSES, "worker" if NUM_PROCESSES == 1 else "workers", len(entries)))

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
        query = "SELECT oeis_id FROM oeis_entries ORDER BY (? - t2) / max(t2 - t1, 1e-6) DESC LIMIT ?;"
        dbcursor.execute(query, (t_current, howmany))
        highest_score_entries = dbcursor.fetchall()
    finally:
        dbcursor.close()

    highest_score_entries = set(oeis_id for (oeis_id, ) in highest_score_entries)
    logger.info("Highest-score entries in local database selected for refresh: {}.".format(len(highest_score_entries)))

    fetch_entries_into_database(dbconn, highest_score_entries)

def vacuum_database(dbconn):
    with start_timer() as timer:
        logger.info("Initiating VACUUM on database ...")
        dbconn.execute("VACUUM;")
        logger.info("VACUUM done in {}.".format(timer.duration_string()))

def database_update_cycle(database_filename):

    while True:

        # Perform an update cycle.

        with start_timer() as timer:

            dbconn = sqlite3.connect(database_filename)

            try:
                ensure_database_schema_created(dbconn)
                highest_oeis_id = find_highest_oeis_id()
                make_database_complete(dbconn, highest_oeis_id)
                #update_database_entries_for_stability()
                update_database_entries_randomly(dbconn, highest_oeis_id // 1000) # refresh 0.1% of entries randomly
                update_database_entries_by_score(dbconn, highest_oeis_id //   50) # refresh 0.5% of entries by score
                vacuum_database(dbconn)
            finally:
                dbconn.close()

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
