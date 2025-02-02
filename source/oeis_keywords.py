#! /usr/bin/env -S python3 -B

"""Check OEIS entries by parsing them and report any issues found."""

import os
import argparse
import sys
import logging
import sqlite3
import concurrent.futures
from typing import Tuple, List
from collections import Counter

from utilities.oeis_entry import parse_oeis_entry, OeisIssue
from utilities.timer import start_timer
from utilities.exit_scope import close_when_done
from utilities.setup_logging import setup_logging

logger = logging.getLogger(__name__)


def process_oeis_entry(oeis_entry: Tuple[int, str, str]):

    (oeis_id, main_content, bfile_content) = oeis_entry

    parsed_oeis_entry = parse_oeis_entry(oeis_id, main_content, bfile_content, lambda x: x)

    return (oeis_id, parsed_oeis_entry.keywords)

def process_database_entries(database_filename: str) -> None:

    if not os.path.exists(database_filename):
        logger.critical("Database file '%s' not found! Unable to continue.", database_filename)
        return

    batch_size = 1000

    keywords = []

    with start_timer() as timer, \
         close_when_done(sqlite3.connect(database_filename)) as db_conn, \
         close_when_done(db_conn.cursor()) as db_cursor, \
         concurrent.futures.ProcessPoolExecutor() as pool:

        # Fetch and process database entries, ordered by oeis_id.

        db_cursor.execute("SELECT oeis_id, main_content, bfile_content FROM oeis_entries ORDER BY oeis_id;")

        while True:

            oeis_entries = db_cursor.fetchmany(batch_size)
            if len(oeis_entries) == 0:
                break

            logger.log(logging.PROGRESS, "Processing OEIS entries A%06d to A%06d ...",
                       oeis_entries[0][0], oeis_entries[-1][0])

            for processed in pool.map(process_oeis_entry, oeis_entries):
                keywords.append(processed)

        logger.info("Processed all database entries in %s.", timer.duration_string())

        with open("keywords.txt", "w") as fo:
            for (oeis_id, kw) in keywords:
                print("A{:06d}  {}".format(oeis_id, ",".join(kw)), file=fo)


def main():

    default_database_filename = "oeis.sqlite3"

    parser = argparse.ArgumentParser(description="Check all OEIS entries in a SQLite3 database.")

    parser.add_argument("-f", dest="filename", type=str, default=default_database_filename, help="OEIS SQLite3 database (default: {})".format(default_database_filename))

    args = parser.parse_args()

    with setup_logging():
        # In recent versions of Python this setting was introduced.
        # We need to increase it from its default value of 4300 to allow all b-files to be processed.
        try:
            sys.set_int_max_str_digits(40000)
        except AttributeError:
            pass

        process_database_entries(args.filename)


if __name__ == "__main__":
    main()
