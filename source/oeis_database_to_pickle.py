#! /usr/bin/env -S python3 -B

"""Write pickled version of an OEIS database."""

import os
import argparse
import sys
import logging
import sqlite3
import concurrent.futures
import pickle

from utilities.oeis_entry import parse_oeis_entry
from utilities.timer import start_timer
from utilities.exit_scope import close_when_done
from utilities.setup_logging import setup_logging

logger = logging.getLogger(__name__)


def process_oeis_entry(oeis_entry):

    (oeis_id, main_content, bfile_content) = oeis_entry

    parsed_entry = parse_oeis_entry(
        oeis_id, main_content, bfile_content,
        lambda issue: logger.warning("A%06d (%s) %s", issue.oeis_id, issue.issue_type.name, issue.description)
    )

    return parsed_entry


def process_database_entries(database_filename: str, pickle_filename: str) -> None:

    if not os.path.exists(database_filename):
        logger.critical("Database file '%s' not found! Unable to continue.", database_filename)
        return

    # Fetch and process database entries, ordered by oeis_id.

    batch_size = 1000

    entries = []

    with start_timer() as timer:
        with close_when_done(sqlite3.connect(database_filename)) as db_conn, close_when_done(db_conn.cursor()) as dbcursor_in, \
             concurrent.futures.ProcessPoolExecutor() as pool:

            dbcursor_in.execute("SELECT oeis_id, main_content, bfile_content FROM oeis_entries ORDER BY oeis_id;")

            while True:

                oeis_entries = dbcursor_in.fetchmany(batch_size)
                if len(oeis_entries) == 0:
                    break

                logger.log(logging.PROGRESS, "Processing OEIS entries A%06d to A%06d ...",
                           oeis_entries[0][0], oeis_entries[-1][0])

                entries.extend(pool.map(process_oeis_entry, oeis_entries))

        logger.info("Processed all database entries in %s.", timer.duration_string())

    with start_timer() as timer:
        with open(pickle_filename, "wb") as fo:
            pickle.dump(entries, fo)
        logger.info("Wrote pickle output file '%s' in %s.", pickle_filename, timer.duration_string())


def main():

    default_database_filename = "oeis.sqlite3"
    default_pickle_output_filename = "oeis.pickle"

    parser = argparse.ArgumentParser(description="Parse main and b-files of all entries in a SQLite3 database.")

    parser.add_argument("-f", dest="filename", type=str, default=default_database_filename, help="OEIS SQLite3 database (default: {})".format(default_database_filename))
    parser.add_argument("--pickle-output-filename", "-o", type=str, default=default_pickle_output_filename, help="output filename (default: '{}')".format(default_pickle_output_filename))

    args = parser.parse_args()

    with setup_logging():
        # In recent versions of Python this setting was introduced.
        # We need to increase it from its default value of 4300 to allow all b-files to be processed.
        try:
            sys.set_int_max_str_digits(40000)
        except AttributeError:
            pass

        process_database_entries(args.filename, args.pickle_output_filename)


if __name__ == "__main__":
    main()
