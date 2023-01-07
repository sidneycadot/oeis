#! /usr/bin/env -S python3 -B

"""Check OEIS entries by parsing them and report any issues found."""

import os
import argparse
import sys
import logging
import sqlite3
import concurrent.futures
from typing import Tuple
from collections import Counter

from utilities.oeis_entry import parse_oeis_entry, OeisIssueType
from utilities.timer import start_timer
from utilities.exit_scope import close_when_done
from utilities.setup_logging import setup_logging

logger = logging.getLogger(__name__)


def process_oeis_entry(oeis_entry: Tuple[int, str, str]):

    (oeis_id, main_content, bfile_content) = oeis_entry

    issues = []

    parse_oeis_entry(oeis_id, main_content, bfile_content, issues.append)

    return issues


def process_database_entries(database_filename: str, lint_output_filename: str) -> None:

    if not os.path.exists(database_filename):
        logger.critical("Database file '%s' not found! Unable to continue.", database_filename)
        return

    batch_size = 1000
    issues = []

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

            logger.log(logging.PROGRESS, "Processing OEIS entries A%06d to A%06d (issues found so far: %d) ...",
                oeis_entries[0][0], oeis_entries[-1][0], len(issues))

            for processed in pool.map(process_oeis_entry, oeis_entries):
                issues.extend(processed)

        logger.info("Processed all database entries in %s (issues found: %d).",
                    timer.duration_string(), len(issues))

        counter = Counter(issue.issue_type for issue in issues)

        logger.info("=== ISSUE TYPE COUNT REPORT ===")
        for (issue_type, count) in counter.most_common():
            logger.info("{:6d} {:3s} - {}".format(count, issue_type.name, issue_type.value))
        logger.info("=== END OF ISSUE TYPE COUNT REPORT ===")

        with open(lint_output_filename, "w") as fo:
            for issue in issues:
                print("{:3s}  A{:06d}  {}".format(issue.issue_type.name, issue.oeis_id, issue.description), file=fo)

        logger.info("Wrote '%s'", lint_output_filename)

def main():

    parser = argparse.ArgumentParser(description="Check all OEIS entries in a SQLite3 database.")

    default_lint_output_filename = "oeislint_output.txt"

    parser.add_argument("-f", dest="filename", type=str, default="oeis.sqlite3", help="OEIS SQLite3 database")
    parser.add_argument("--lint-output-filename", "-o", type=str, default=default_lint_output_filename, help="output filename (default: '{}')".format(default_lint_output_filename))

    args = parser.parse_args()

    with setup_logging():
        # In recent versions of Python this setting was introduced.
        # We need to increase it from its default value of 4300 to allow all b-files to be processed.
        try:
            sys.set_int_max_str_digits(40000)
        except AttributeError:
            pass

        process_database_entries(args.filename, args.lint_output_filename)


if __name__ == "__main__":
    main()
