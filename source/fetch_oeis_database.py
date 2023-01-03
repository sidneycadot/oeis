#! /usr/bin/env -S python3 -B

"""This script fetches the remote OEIS database to a local SQLite3 database.

The script is designed to run indefinitely. When all remote data is fetched, the script
will periodically refresh its local entries.

Once every month, a consolidated version of the SQLite database will be compressed and
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
from typing import Set

from utilities.fetch_remote_oeis_entry import fetch_remote_oeis_entry, BadOeisResponse
from utilities.timer import start_timer
from utilities.exit_scope import close_when_done
from utilities.setup_logging import setup_logging

logger = logging.getLogger(__name__)


def ensure_database_schema_created(db_conn):
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

    db_conn.execute(schema)


def find_highest_oeis_id():
    """Find the highest entry ID in the remote OEIS database by performing HTTP queries and doing a binary search."""

    sleep_after_failure = 5.0

    success_id = 350000            # We know a-priori that this entry exists.
    failure_id = 100 * success_id  # We know a-priori that this entry does not exist.

    # Do a binary search, looking for the success/failure boundary.
    while success_id + 1 != failure_id:

        fetch_id = (success_id + failure_id) // 2

        logger.info("OEIS search range is (%d, %d), attempting to fetch entry %d ...", success_id, failure_id, fetch_id)

        try:
            fetch_remote_oeis_entry(fetch_id, fetch_bfile_flag = False)
        except BadOeisResponse:
            # This exception happens when trying to read beyond the last entry in the database.
            # We mark the failure and continue the binary search.
            logging.info("OEIS entry %d does not exist.", fetch_id)
            failure_id = fetch_id
        except BaseException as exception:
            # Some other error occurred. We have to retry.
            logger.error("Unexpected fetch result (%s), retrying in %f seconds.", exception, sleep_after_failure)
            time.sleep(sleep_after_failure)
        else:
            # We mark the success and continue the binary search.
            logging.info("OEIS entry %d exists.", fetch_id)
            success_id = fetch_id

    logger.info("Last valid OEIS entry is A%06d.", success_id)

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
        logger.error("Unable to fetch entry %d: '%s'.", entry, exception)
        result = None
    return result


def process_responses(db_conn, responses) -> Set[int]:
    """Process a batch of responses by updating the local SQLite database.

    A logging message is produced that summarizes how the batch of responses was processed.
    This function returns a set of OEIS IDs that have been successfully processed.
    """

    count_failures          = 0
    count_new_entries       = 0
    count_identical_entries = 0
    count_updated_entries   = 0

    processed_entries = set()

    with close_when_done(db_conn.cursor()) as db_cursor:

        for response in responses:

            if response is None:
                # Skip entries that are not okay.
                # Do not record the failures in the processed_entries set.
                count_failures += 1
                continue

            query = "SELECT main_content, bfile_content FROM oeis_entries WHERE oeis_id = ?;"
            db_cursor.execute(query, (response.oeis_id, ))

            previous_content = db_cursor.fetchall()

            assert len(previous_content) <= 1
            previous_content = None if len(previous_content) == 0 else previous_content[0]

            if previous_content is None:
                # The oeis_id does not occur in the database yet.
                # We will insert it as a new entry.
                query = "INSERT INTO oeis_entries(oeis_id, t1, t2, main_content, bfile_content) VALUES (?, ?, ?, ?, ?);"
                db_cursor.execute(query, (response.oeis_id, response.timestamp, response.timestamp, response.main_content, response.bfile_content))
                count_new_entries += 1
            elif previous_content != (response.main_content, response.bfile_content):
                # The database content is stale.
                # Update t1, t2, and content.
                query = "UPDATE oeis_entries SET t1 = ?, t2 = ?, main_content = ?, bfile_content = ? WHERE oeis_id = ?;"
                db_cursor.execute(query, (response.timestamp, response.timestamp, response.main_content, response.bfile_content, response.oeis_id))
                count_updated_entries += 1
            else:
                # The database content is identical to the freshly fetched content.
                # We will just update the t2 field, indicating the fresh fetch.
                query = "UPDATE oeis_entries SET t2 = ? WHERE oeis_id = ?;"
                db_cursor.execute(query, (response.timestamp, response.oeis_id))
                count_identical_entries += 1

            processed_entries.add(response.oeis_id)

    db_conn.commit()

    response_noun = "response" if len(responses) == 1 else "responses"
    logger.info("Processed %d %s (failures: %d, new: %d, identical: %d, updated: %d).",
                len(responses), response_noun,
                count_failures, count_new_entries, count_identical_entries, count_updated_entries)

    return processed_entries


def fetch_entries_into_database(db_conn, entries) -> None:
    """Fetch a set of entries from the remote OEIS database and store the results in the database.

    The 'entries' parameter contains a number of OEIS IDs.
    This function can handle a large number of entries, up to the entire size of the OEIS database.

    Entries are processed in randomized batches.
    We use a pool of worker threads to perform the actual fetches.
    This enhances fetch performance (in terms of fetches-per-second) dramatically.
    Typical fetch performance is about 20 fetches per second.

    The responses of each batch are processed by the 'process_responses' function defined above.
    """

    fetch_batch_size = 500   # 100 -- 1000 are reasonable
    num_workers = 20         # 10 -- 20 are reasonable
    sleep_after_batch = 2.0  # in [seconds]

    remaining_entries = set(entries)

    with start_timer(len(remaining_entries)) as timer, concurrent.futures.ThreadPoolExecutor(num_workers) as executor:

        while len(remaining_entries) > 0:

            batch_size = min(fetch_batch_size, len(remaining_entries))

            batch = random.sample(list(remaining_entries), batch_size)

            worker_noun = "worker" if num_workers == 1 else "workers"
            entry_noun = "entry" if len(remaining_entries) == 1 else "entries"

            logger.info("Fetching data using %d %s for %d out of %d %s ...",
                        num_workers, worker_noun, batch_size, len(remaining_entries), entry_noun)

            with start_timer() as batch_timer:

                # Execute the fetches in parallel.
                responses = list(executor.map(safe_fetch_remote_oeis_entry, batch))

                fetch_noun = "fetch" if batch_size == 1 else "fetches"

                logger.info("%d %s took %s (%.3f fetches/second).",
                            batch_size, fetch_noun,
                            batch_timer.duration_string(), batch_size / batch_timer.duration())

            # Process the responses by updating the database.

            processed_entries = process_responses(db_conn, responses)

            remaining_entries -= processed_entries

            # Calculate and show estimated-time-to-completion.

            logger.info("Estimated time to completion: %s.", timer.etc_string(work_remaining = len(remaining_entries)))

            # Sleep if we need to fetch more.
            if len(remaining_entries) > 0:
                logger.info("Sleeping for %.1f seconds ...", sleep_after_batch)
                time.sleep(sleep_after_batch)

        entry_noun = "entry" if timer.total_work == 1 else "entries"

        logger.info("Fetched %d %s in %s.", timer.total_work, entry_noun, timer.duration_string())


def make_database_complete(db_conn, highest_oeis_id: int) -> None:
    """Fetch all entries from the remote OEIS database that are not yet present in the local SQLite database."""

    with close_when_done(db_conn.cursor()) as db_cursor:
        db_cursor.execute("SELECT oeis_id FROM oeis_entries;")
        present_entries = db_cursor.fetchall()

    present_entries = [oeis_id for (oeis_id, ) in present_entries]
    logger.info("Entries present in local database: %d.", len(present_entries))

    all_entries = range(1, highest_oeis_id + 1)

    missing_entries = set(all_entries) - set(present_entries)
    logger.info("Missing entries to be fetched: %d.", len(missing_entries))

    fetch_entries_into_database(db_conn, missing_entries)


def update_database_manual_fetch(db_conn, filename: str, highest_oeis_id: int) -> None:
    """Fetch entries specified in a file."""

    fetch_entries = set()
    try:
        with open(filename, "r") as fi:
            for line in fi:
                line = line.strip()
                if len(line) != 0 and not line.startswith("#"):
                    try:
                        entry = int(line)
                        if 1 <= entry <= highest_oeis_id:
                            fetch_entries.add(entry)
                    except ValueError:
                        logger.warning("Ignored bad entry in '%s' file: '%s'", filename, line)
    except FileNotFoundError:
        logger.info("File '%s' not found, skipping manual fetch step.", filename)
        return

    logger.info("Manually specified entries to be refreshed as specified in '%s': %d.", filename, len(fetch_entries))

    fetch_entries_into_database(db_conn, fetch_entries)

    logging.info("Removing file '%s' ...", filename)
    os.remove(filename)


def update_database_entries_randomly(db_conn, max_count: int) -> None:
    """Re-fetch (update) a random subset of entries that are already present in the local SQLite database."""

    with close_when_done(db_conn.cursor()) as db_cursor:
        db_cursor.execute("SELECT oeis_id FROM oeis_entries;")
        present_entries = db_cursor.fetchall()

    present_entries = [oeis_id for (oeis_id, ) in present_entries]

    random_entries_count = min(max_count, len(present_entries))

    random_entries = random.sample(present_entries, random_entries_count)

    logger.info("Random entries in local database selected for refresh: %d.", len(random_entries))

    fetch_entries_into_database(db_conn, random_entries)


def update_database_entries_by_priority(db_conn, max_count: int):
    """Re-fetch entries that are old, relative to their stability.

    For each entry, a priority is determined, as follows:

    The _age_ is defined as the number of seconds ago that the entry was last fetched in its current state.
    The _stability_ is defined as the number of seconds between the first and last fetches in the current state.
    The _priority_ is the _age_ divided by the _stability_.

    A high priority indicates that the entry is old and/or unstable.
    Such entries are fetched in preference to entries that are recent and/or stable (and have a lower priority).
    """

    t_current = time.time()

    with close_when_done(db_conn.cursor()) as dbcursor:
        query = "SELECT oeis_id FROM oeis_entries ORDER BY (? - t2) / max(t2 - t1, 1e-6) DESC LIMIT ?;"
        dbcursor.execute(query, (t_current, max_count))
        highest_priority_entries = dbcursor.fetchall()

    highest_priority_entries = [oeis_id for (oeis_id, ) in highest_priority_entries]

    logger.info("Highest-priority entries in local database selected for refresh: %d.", len(highest_priority_entries))

    fetch_entries_into_database(db_conn, highest_priority_entries)


def update_database_entries_for_nonzero_time_window(db_conn):
    """Re-fetch entries in the database that have a zero-second time window.

    These are entries that have been fetched only once so far.
    """

    while True:

        with close_when_done(db_conn.cursor()) as dbcursor:
            dbcursor.execute("SELECT oeis_id FROM oeis_entries WHERE t1 = t2;")
            zero_time_window_entries = dbcursor.fetchall()

        if len(zero_time_window_entries) == 0:
            break  # no zero-time_window entries.

        zero_time_window_entries = [oeis_id for (oeis_id, ) in zero_time_window_entries]

        logger.info("Entries with zero time window in local database selected for refresh: %d.", len(zero_time_window_entries))

        fetch_entries_into_database(db_conn, zero_time_window_entries)


def vacuum_database(db_conn) -> None:
    """Perform a VACUUM command on the database."""

    with start_timer() as timer:
        logger.info("Initiating VACUUM on database ...")
        db_conn.execute("VACUUM;")
        logger.info("VACUUM done in %s.", timer.duration_string())


def compress_file(from_filename: str, to_filename: str) -> None:
    """Compress a file using the LZMA compression algorithm, and save it in xz format."""

    compression_preset = 9  # Level 9 without 'extra' works best on our data.
    block_size = 1048576    # Process data in blocks of 1 megabyte.

    with start_timer() as timer:
        logger.info("Compressing data from '%s' to '%s' ...", from_filename, to_filename)
        with open(from_filename, "rb") as fi, \
             lzma.open(to_filename, "wb", format = lzma.FORMAT_XZ, check = lzma.CHECK_CRC64, preset = compression_preset) as fo:
            while True:
                data = fi.read(block_size)
                if len(data) == 0:
                    break
                fo.write(data)
        logger.info("Compressing data took %s.", timer.duration_string())


def consolidate_database_monthly(database_filename: str, remove_stale_files_flag: bool) -> None:
    """Make a consolidated version of the database once every month.

    The consolidated version will have a standardized filename 'oeis_vYYYYMMDD.sqlite3.xz'.

    If this filename already exists, we return immediately.

    If not, we vacuum the database, and compress its file. This process takes ~ 2 hours on a fast desktop PC.

    When the compressed database is written, we optionally remove all 'stale' consolidated files,
    i.e., all files that are called 'oeis_vYYYYMMDD.sqlite3.xz' except the one we just wrote.
    """

    now = datetime.datetime.now()

    if now.day != 1:
        return

    xz_filename = now.strftime("oeis_v%Y%m%d.sqlite3.xz")
    if os.path.exists(xz_filename):
        return  # The file already exists.

    with start_timer() as timer:

        logger.info("Consolidating database to '%s ...", xz_filename)

        # Vacuum the database.
        with close_when_done(sqlite3.connect(database_filename)) as db_conn:
            vacuum_database(db_conn)

        # Create the xz file.
        compress_file(database_filename, xz_filename)

        # Remove stale files.
        if remove_stale_files_flag:
            stale_files = [filename for filename in glob.glob("oeis_v????????.sqlite3.xz") if filename != xz_filename]
            for filename in stale_files:
                logger.info("Removing stale consolidated file '%s' ...", filename)
                os.remove(filename)

        logger.info("Consolidating data took %s.", timer.duration_string())


def database_update_cycle(database_filename: str) -> None:
    """Perform a single cycle of the database update loop."""

    with start_timer() as timer:

        # Check the OEIS server for the highest entry ID present.
        highest_oeis_id = find_highest_oeis_id()

        with close_when_done(sqlite3.connect(database_filename)) as db_conn:
            # Ensure that the database is properly initialized.
            ensure_database_schema_created(db_conn)
            # Make sure we have all entries (full fetch on first run).
            make_database_complete(db_conn, highest_oeis_id)
            # Refresh entries found in the "oeis_fetch.txt" file.
            update_database_manual_fetch(db_conn, "oeis_fetch.txt", highest_oeis_id)
            # Refresh 0.1 % of entries randomly.
            update_database_entries_randomly(db_conn, highest_oeis_id // 1000)
            # Refresh 0.5 % of entries by priority.
            update_database_entries_by_priority(db_conn, highest_oeis_id // 200)
            # Make sure we have t1 != t2 for all entries (full fetch on first run).
            update_database_entries_for_nonzero_time_window(db_conn)

        consolidate_database_monthly(database_filename, remove_stale_files_flag = False)

        logger.info("Full database update cycle took %s.", timer.duration_string())


def database_update_cycle_loop(database_filename: str) -> None:
    """Call the database update cycle in an infinite loop, with random pauses in between."""

    while True:

        try:
            database_update_cycle(database_filename)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt request received, ending database update cycle loop...")
            break
        except BaseException as exception:
            logger.error("Error while performing database update cycle: '%s'.", exception)

        # Pause between update cycles.
        pause = max(300.0, random.gauss(1800.0, 600.0))
        logger.info("Sleeping for %.1f seconds ...", pause)
        time.sleep(pause)


def main():
    """Initialize logger and run the database update cycle loop."""

    database_filename = "oeis.sqlite3"

    logfile = "logfiles/fetch_oeis_database_%Y%m%d_%H%M%S.log"

    with setup_logging(logfile):
        database_update_cycle_loop(database_filename)


if __name__ == "__main__":
    main()
