#! /usr/bin/env -S python3 -B

"""Process raw OEIS entries by parsing them, checking many constraints in the process."""

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

    parsed_entry = parse_oeis_entry(oeis_id, main_content, bfile_content)

    return parsed_entry


def process_database_entries(database_filename_in: str) -> None:

    if not os.path.exists(database_filename_in):
        logger.critical("Database file '{}' not found! Unable to continue.".format(database_filename_in))
        return

    (root, ext) = os.path.splitext(database_filename_in)

    pickle_filename_out = root + "_parsed.pickle"

    if os.path.exists(pickle_filename_out):
        logger.info("Removing stale file '{}' ...".format(pickle_filename_out))
        os.remove(pickle_filename_out)

    # ========== fetch and process database entries, ordered by oeis_id.

    batch_size = 1000

    entries = []

    with start_timer() as timer:
        with close_when_done(sqlite3.connect(database_filename_in)) as dbconn_in, close_when_done(dbconn_in.cursor()) as dbcursor_in, \
             concurrent.futures.ProcessPoolExecutor() as pool:

            dbcursor_in.execute("SELECT oeis_id, main_content, bfile_content FROM oeis_entries ORDER BY oeis_id LIMIT 21000;")

            while True:

                oeis_entries = dbcursor_in.fetchmany(batch_size)
                if len(oeis_entries) == 0:
                    break

                logger.log(logging.PROGRESS, "Processing OEIS entries A{:06} to A{:06} ...".format(oeis_entries[0][0], oeis_entries[-1][0]))

                entries.extend(pool.map(process_oeis_entry, oeis_entries))

        logger.info("Processed all database entries in {}.".format(timer.duration_string()))

    with open(pickle_filename_out, "wb") as fo:
        pickle.dump(entries, fo)


def main():

    parser = argparse.ArgumentParser(description="Parse main and b-files of all entries in a SQLite3 database.")

    parser.add_argument("filename", type=str, default="oeis.sqlite3")

    args = parser.parse_args()

    (root, ext) = os.path.splitext(args.filename)
    logfile = root + "_parsed.log"

    with setup_logging(logfile):
        # In recent versions of Python this setting was introduced.
        # We need to increase it from its default value of 4300 to allow all b-files to be processed.
        try:
            sys.set_int_max_str_digits(40000)
        except AttributeError:
            pass

        process_database_entries(args.filename)


if __name__ == "__main__":
    main()
