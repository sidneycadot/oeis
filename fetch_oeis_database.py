#! /usr/bin/env python3

"""This script is used to fetch the remote OEIS database to a local SQLite3 database.

The script is designed to run indefinitely. When all remote data is fetched, the script
will periodically refresh its local entries.

Once every day, a consolidated version of the SQLite database will be compressed and
written to the local directory. This file will be called "oeis_vYYYYMMDD.sqlite3.xz".
Stale versions of this consolidated file will be removed automatically.
"""

import os
import glob
import datetime
import time
import sqlite3
import random
import logging
import lzma
import concurrent.futures

from fetch_remote_oeis_entry import fetch_remote_oeis_entry, BadOeisResponse
from timer import start_timer
from exit_scope  import close_when_done
from setup_logging import setup_logging

logger = logging.getLogger(__name__)


def ensure_database_schema_created(conn):
    """Ensure that the 'oeis_entries' table is present in the database.

    Note that the database schema has the "IF NOT EXISTS" clause.
    This ensures that the statement will be ignored if the table is already present.
    """

    schema = """
             CREATE TABLE IF NOT EXISTS oeis_entries (
                 oeis_id       INTEGER  PRIMARY KEY NOT NULL, -- OEIS ID number.
                 t1            REAL                 NOT NULL, -- earliest timestamp when the content below was first fetched.
                 t2            REAL                 NOT NULL, -- most recent timestamp when the content below was fetched.
                 main_content  TEXT                 NOT NULL, -- main content (i.e., lines starting with '%' sign).
                 bfile_content TEXT                 NOT NULL  -- b-file content (secondary file containing sequence entries).
             );
             """

    # Remove the first 13 characters of each line of the SQL statement above, as well as the first and last lines.

    schema = "\n".join(line[13:] for line in schema.split("\n"))[1:-1]

    # Execute the schema creation statement.

    conn.execute(schema)


def find_highest_oeis_id():
    """Find the highest entry ID in the remote OEIS database by performing HTTP queries and doing a binary search."""

    SLEEP_AFTER_FAILURE = 5.0

    success_id =  263000 # We know a-priori that this entry exists.
    failure_id = 1000000 # We know a-priori that this entry does not exist.

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
    """Fetch a single OEIS entry from the remote OEIS database, and swallow any exceptions.

    If no issues are encountered, this function is identical to the 'fetch_remote_oeis_entry' function.
    In case of an exception, a log message is generated and 'None' is returned.

    The purpose of this function in to be used in a "map", where we want to inhibit exceptions.
    """

    # Intercepts and reports any exceptions.
    # In case of an exception, a log message is generated, and None is returned.
    try:
        result = fetch_remote_oeis_entry(entry, True)
    except BaseException as exception:
        logger.error("Unable to fetch entry {}: '{}'.".format(entry, exception))
        result = None
    return result


def process_responses(conn, responses):
    """Process a batch of responses by updating the local SQLite database.

    A logging message is produced that summarizes how the batch of responses was processed.
    This function returns a set of OEIS IDs that have been successfully processed.
    """

    count_failures          = 0
    count_new_entries       = 0
    count_identical_entries = 0
    count_updated_entries   = 0

    processed_entries = set()

    with close_when_done(conn.cursor()) as dbcursor:

        for response in responses:

            if response is None:
                # Skip entries that are not okay.
                # Do not record the failures in the processed_entries set.
                count_failures += 1
                continue

            query = "SELECT main_content, bfile_content FROM oeis_entries WHERE oeis_id = ?;"
            dbcursor.execute(query, (response.oeis_id, ))

            previous_content = dbcursor.fetchall()

            assert len(previous_content) <= 1
            previous_content = None if len(previous_content) == 0 else previous_content[0]

            if previous_content is None:
                # The oeis_id does not occur in the database yet.
                # We will insert it as a new entry.
                query = "INSERT INTO oeis_entries(oeis_id, t1, t2, main_content, bfile_content) VALUES (?, ?, ?, ?, ?);"
                dbcursor.execute(query, (response.oeis_id, response.timestamp, response.timestamp, response.main_content, response.bfile_content))
                count_new_entries += 1
            elif previous_content != (response.main_content, response.bfile_content):
                # The database content is stale.
                # Update t1, t2, and content.
                query = "UPDATE oeis_entries SET t1 = ?, t2 = ?, main_content = ?, bfile_content = ? WHERE oeis_id = ?;"
                dbcursor.execute(query, (response.timestamp, response.timestamp, response.main_content, response.bfile_content, response.oeis_id))
                count_updated_entries += 1
            else:
                # The database content is identical to the freshly fetched content.
                # We will just update the t2 field, indicating the fresh fetch.
                query = "UPDATE oeis_entries SET t2 = ? WHERE oeis_id = ?;"
                dbcursor.execute(query, (response.timestamp, response.oeis_id))
                count_identical_entries += 1

            processed_entries.add(response.oeis_id)

    conn.commit()

    logger.info("Processed {} responses (failures: {}, new: {}, identical: {}, updated: {}).".format(len(responses), count_failures, count_new_entries, count_identical_entries, count_updated_entries))

    return processed_entries


def fetch_entries_into_database(conn, entries):
    """Fetch a set of entries from the remote OEIS database and store the results in the database.

    The 'entries' parameter contains a number of OEIS IDs.
    This function can handle a large number of entries, up to the entire size of the OEIS database.

    Entries are processed in randomized batches.
    We use a pool of worker threads to perform the actual fetches.
    This enhances fetch performance (in terms of fetches-per-second) dramatically.
    Typical fetch performance is about 20 fetches per second.

    The responses of each batch are processed by the 'process_responses' function defined above.
    """

    FETCH_BATCH_SIZE  = 500  # 100 -- 1000 are reasonable
    NUM_WORKERS       = 20   # 10 -- 20 are reasonable
    SLEEP_AFTER_BATCH = 2.0  # [seconds]

    entries = set(entries)  # make a copy, and ensure it is a set.

    with start_timer(len(entries)) as timer, concurrent.futures.ThreadPoolExecutor(NUM_WORKERS) as executor:

        while len(entries) > 0:

            batch_size = min(FETCH_BATCH_SIZE, len(entries))

            batch = random.sample(entries, batch_size)

            logger.info("Fetching data using {} {} for {} out of {} entries ...".format(NUM_WORKERS, "worker" if NUM_WORKERS == 1 else "workers", batch_size, len(entries)))

            with start_timer() as batch_timer:

                # Execute fetches in parallel.
                responses = list(executor.map(safe_fetch_remote_oeis_entry, batch))

                logger.info("{} fetches took {} ({:.3f} fetches/second).".format(batch_size, batch_timer.duration_string(), batch_size / batch_timer.duration()))

            # Process the responses by updating the database.

            processed_entries = process_responses(conn, responses)

            entries -= processed_entries

            # Calculate and show estimated-time-to-completion.

            logger.info("Estimated time to completion: {}.".format(timer.etc_string(work_remaining = len(entries))))

            # Sleep if we need to fetch more
            if len(entries) > 0:
                logger.info("Sleeping for {:.1f} seconds ...".format(SLEEP_AFTER_BATCH))
                time.sleep(SLEEP_AFTER_BATCH)

        logger.info("Fetched {} entries in {}.".format(timer.total_work, timer.duration_string()))


def make_database_complete(conn, highest_oeis_id):
    """Fetch all entries from the remote OEIS database that are not yet present in the local SQLite database."""

    with close_when_done(conn.cursor()) as dbcursor:
        dbcursor.execute("SELECT oeis_id FROM oeis_entries;")
        present_entries = dbcursor.fetchall()

    present_entries = [oeis_id for (oeis_id, ) in present_entries]
    logger.info("Entries present in local database: {}.".format(len(present_entries)))

    all_entries = range(1, highest_oeis_id + 1)

    missing_entries = set(all_entries) - set(present_entries)
    logger.info("Missing entries to be fetched: {}.".format(len(missing_entries)))

    fetch_entries_into_database(conn, missing_entries)


def update_database_entries_randomly(conn, max_count):
    """Re-fetch (update) a random subset of entries that are already present in the local SQLite database."""

    with close_when_done(conn.cursor()) as dbcursor:
        dbcursor.execute("SELECT oeis_id FROM oeis_entries;")
        present_entries = dbcursor.fetchall()

    present_entries = [oeis_id for (oeis_id, ) in present_entries]

    random_entries_count = min(max_count, len(present_entries))

    random_entries = random.sample(present_entries, random_entries_count)

    logger.info("Random entries in local database selected for refresh: {}.".format(len(random_entries)))

    fetch_entries_into_database(conn, random_entries)


def update_database_entries_by_priority(conn, max_count):
    """Re-fetch entries that are old, relative to their stability.

    For each entry, a priority is determined, as follows:

    The _age_ is defined as the number of seconds ago that the entry was last fetched in its current state.
    The _stability_ is defined as the number of seconds between the first and last fetches in the current state.
    The _priority_ is the _age_ divided by the _stability_.

    A high priority indicates that the entry is old and/or unstable.
    Such entries are fetched in preference to entries that are recent and/or stable (and have a lower priority).
    """

    t_current = time.time()

    with close_when_done(conn.cursor()) as dbcursor:
        query = "SELECT oeis_id FROM oeis_entries ORDER BY (? - t2) / max(t2 - t1, 1e-6) DESC LIMIT ?;"
        dbcursor.execute(query, (t_current, max_count))
        highest_priority_entries = dbcursor.fetchall()

    highest_priority_entries = [oeis_id for (oeis_id, ) in highest_priority_entries]

    logger.info("Highest-priority entries in local database selected for refresh: {}.".format(len(highest_priority_entries)))

    fetch_entries_into_database(conn, highest_priority_entries)


def update_database_entries_for_nonzero_time_window(conn):
    """ Re-fetch entries in the database that have a 0-second time window. These are entries that have been fetched only once."""

    while True:

        with close_when_done(conn.cursor()) as dbcursor:
            dbcursor.execute("SELECT oeis_id FROM oeis_entries WHERE t1 = t2;")
            zero_time_window_entries = dbcursor.fetchall()

        if len(zero_time_window_entries) == 0:
            break  # no zero-time_window entries.

        zero_time_window_entries = [oeis_id for (oeis_id, ) in zero_time_window_entries]

        logger.info("Entries with zero time window in local database selected for refresh: {}.".format(len(zero_time_window_entries)))

        fetch_entries_into_database(conn, zero_time_window_entries)


def vacuum_database(conn):
    """Perform a VACUUM command on the database."""

    with start_timer() as timer:
        logger.info("Initiating VACUUM on database ...")
        conn.execute("VACUUM;")
        logger.info("VACUUM done in {}.".format(timer.duration_string()))


def compress_file(from_filename, to_filename):
    """Compress a file using the 'xz' compression algorithm."""

    PRESET = 9  # level 9 without 'extra' works best on our data.

    BLOCKSIZE = 1048576  # Process data in blocks of 1 megabyte.

    with start_timer() as timer:
        logger.info("Compressing data from '{}' to '{}' ...".format(from_filename, to_filename))
        with open(from_filename, "rb") as fi, lzma.open(to_filename, "wb", format = lzma.FORMAT_XZ, check = lzma.CHECK_CRC64, preset = PRESET) as fo:
            while True:
                data = fi.read(BLOCKSIZE)
                if len(data) == 0:
                    break
                fo.write(data)
        logger.info("Compressing data took {}.".format(timer.duration_string()))


def consolidate_database_monthly(database_filename, remove_stale_files_flag):
    """Make a consolidated version of the database once every month.

    The consolidated version will have a standardized filename 'oeis_vYYYYMMDD.sqlite3.xz'.

    If this filename already exists, we return immediately.

    If not, we vacuum the database, and compress its file. This process takes ~ 2 hours on a fast desktop PC.

    When the compressed database is written, we remove all 'stale' consolidated files,
    i.e., all files that are called 'oeis_vYYYYMMDD.sqlite3.xz' except the one we just wrote.
    """

    now = datetime.datetime.now()

    if now.day != 1:
        return

    xz_filename = now.strftime("oeis_v%Y%m%d.sqlite3.xz")
    if os.path.exists(xz_filename):
        return # file already exists.

    with start_timer() as timer:

        logger.info("Consolidating database to '{}' ...".format(xz_filename))

        # Vacuum the database
        with close_when_done(sqlite3.connect(database_filename)) as conn:
            vacuum_database(conn)

        # Create the xz file.
        compress_file(database_filename, xz_filename)

        # Remove stale files.
        if remove_stale_files_flag:
            stale_files = [filename for filename in glob.glob("oeis_v????????.sqlite3.xz") if filename != xz_filename]
            for filename in stale_files:
                logger.info("Removing stale consolidated file '{}' ...".format(filename))
                os.remove(filename)

        logger.info("Consolidating data took {}.".format(timer.duration_string()))


def database_update_cycle(database_filename):
    """Perform a single cycle of the database update loop."""

    with start_timer() as timer:

        highest_oeis_id = find_highest_oeis_id() # Check OEIS server for highest entry ID.

        with close_when_done(sqlite3.connect(database_filename)) as dbconn:
            ensure_database_schema_created(dbconn)
            make_database_complete(dbconn, highest_oeis_id)                      # Make sure we have all entries (full fetch on first run).
            update_database_entries_randomly(dbconn, highest_oeis_id // 1000)    # Refresh 0.1 % of entries randomly.
            update_database_entries_by_priority(dbconn, highest_oeis_id // 200)  # Refresh 0.5 % of entries by priority.
            update_database_entries_for_nonzero_time_window(dbconn)              # Make sure we have t1 != t2 for all entries (full fetch on first run).

        consolidate_database_monthly(database_filename, remove_stale_files_flag = False)

        logger.info("Full database update cycle took {}.".format(timer.duration_string()))


def database_update_cycle_loop(database_filename):
    """Call the database update cycle in an infinite loop, with random pauses in between."""

    while True:

        try:
            database_update_cycle(database_filename)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt request received, ending database update cycle loop...")
            break
        except BaseException as exception:
            logger.error("Error while performing database update cycle: '{}'.".format(exception))

        # Pause between update cycles.
        pause = max(300.0, random.gauss(1800.0, 600.0))
        logger.info("Sleeping for {:.1f} seconds ...".format(pause))
        time.sleep(pause)


def main():
    """Initialize logger and run the database update cycle loop."""

    database_filename = "oeis.sqlite3"

    logfile = "logfiles/fetch_oeis_database_%Y%m%d_%H%M%S.log"

    with setup_logging(logfile):
        database_update_cycle_loop(database_filename)


if __name__ == "__main__":
    main()
