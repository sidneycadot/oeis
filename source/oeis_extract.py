#! /usr/bin/env -S python3 -B

"""Write pickled version of an OEIS database."""

import os
import argparse
import sys
import logging
import sqlite3
import concurrent.futures
import pickle
from typing import List

from utilities.oeis_entry import parse_oeis_entry
from utilities.timer import start_timer
from utilities.exit_scope import close_when_done
from utilities.setup_logging import setup_logging

logger = logging.getLogger(__name__)


def extract_database_entry(database_filename: str, oeis_ids: List[int]) -> None:

    if not os.path.exists(database_filename):
        logger.critical("Database file '%s' not found! Unable to continue.", database_filename)
        return

    with close_when_done(sqlite3.connect(database_filename)) as db_conn, close_when_done(db_conn.cursor()) as db_cursor:

        query = "SELECT oeis_id, main_content, bfile_content FROM oeis_entries WHERE oeis_id IN ({}) ORDER BY oeis_id;".format(", ".join(map(str, oeis_ids)))
        db_cursor.execute(query)

        oeis_entries = db_cursor.fetchall()

        for (oeis_id, main_content, bfile_content) in oeis_entries:
            logger.info("Writing a{:06d}.txt ...".format(oeis_id))
            with open("a{:06}.txt".format(oeis_id), "w") as fo:
                fo.write(main_content)
            logger.info("Writing b{:06d}.txt ...".format(oeis_id))
            with open("b{:06}.txt".format(oeis_id), "w") as fo:
                fo.write(bfile_content)

def main():

    default_database_filename = "oeis.sqlite3"

    parser = argparse.ArgumentParser(description="Extract main file and b-file of selected OEIS entries from the database.")

    parser.add_argument("-f", dest="filename", type=str, default=default_database_filename, help="OEIS SQLite3 database (default: {})".format(default_database_filename))
    parser.add_argument("oeis_ids", metavar="oeis_id", nargs="+", type=int, help="OEIS ID to be extracted")

    args = parser.parse_args()

    with setup_logging():
        extract_database_entry(args.filename, args.oeis_ids)


if __name__ == "__main__":
    main()
