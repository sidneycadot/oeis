#! /usr/bin/env -S python3 -B -u

"""Write pickled version of an OEIS database."""

import re
import os
import argparse
import sys
import logging
import sqlite3
import concurrent.futures
import pickle
from contextlib import redirect_stdout
from typing import List

from utilities.oeis_entry import parse_oeis_entry, parse_main_content_directives
from utilities.timer import start_timer
from utilities.exit_scope import close_when_done
from utilities.setup_logging import setup_logging
from collections import Counter

logger = logging.getLogger(__name__)


def to_characters(s: str) -> str:
    return "".join(sorted(set(s)))


def process_database_entries(database_filename: str) -> None:

    if not os.path.exists(database_filename):
        logger.critical("Database file '%s' not found! Unable to continue.", database_filename)
        return

    # Fetch and process database entries, ordered by oeis_id.

    batch_size = 1000

    with start_timer() as timer:

        directive_data = {}

        with close_when_done(sqlite3.connect(database_filename)) as db_conn, close_when_done(db_conn.cursor()) as db_cursor, \
             concurrent.futures.ProcessPoolExecutor() as pool:

            db_cursor.execute("SELECT oeis_id, main_content, bfile_content FROM oeis_entries ORDER BY oeis_id;")

            while True:

                oeis_entries = db_cursor.fetchmany(batch_size)
                if len(oeis_entries) == 0:
                    break

                logger.log(logging.PROGRESS, "Processing OEIS entries A%06d to A%06d ...",
                           oeis_entries[0][0], oeis_entries[-1][0])

                for (oeis_id, main_content, bfile_content) in oeis_entries:

                    directives = parse_main_content_directives(oeis_id, main_content)

                    for (directive, content) in directives:
                        if content.startswith(" "):
                            content = content[1:]

                        assert len(directive) == 1

                        if directive in ("S", "T", "U"):
                            directive = "STU"
                        elif directive in ("V", "W", "X"):
                            directive = "VWX"

                        max_oeis_entries = 10

                        for c in set(content):
                            if directive not in directive_data:
                                directive_data[directive] = {}
                            if c not in directive_data[directive]:
                                directive_data[directive][c] = set()  # Set of OEIS IDs where this directive/character combination occurs.
                            if len(directive_data[directive][c]) < max_oeis_entries:
                                directive_data[directive][c].add(oeis_id)

                    # Check b-file content

                    #lines = bfile_content.splitlines()

                    #for line in lines:
                        
                    #    if len(line) == 0:
                    #        continue

                    #    first_character = line[0]
                    #    if first_character in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9'):
                    #        directive = "bfile:digit"
                    #    elif first_character == "#":
                    #        directive = "bfile:#"
                    #    else:
                    #        directive = "bfile:" + repr(first_character)

                    #    content = line[1:]

                    #    if directive not in counters:
                    #        counters[directive] = Counter()
                    #    counters[directive].update(content)

        logger.info("Processed all database entries in %s.", timer.duration_string())

    filename = "verify_characters_output.txt"

    logger.info("Writing file '%s' ...", filename)
    
    with open(filename, "w") as fo, redirect_stdout(fo):
        for directive in sorted(directive_data):
            for c in sorted(directive_data[directive]):
                print("%{:3s}  u{:08x}  {:20s}  {}".format(directive, ord(c), repr(c), sorted(directive_data[directive][c])))

    filename = "occurring_characters.py"

    logger.info("Writing file '%s' ...", filename)

    with open(filename, "w") as fo, redirect_stdout(fo):
        print("occurring_characters_per_directive = {")
        for (directive_index, directive) in enumerate(sorted(directive_data)):
            seperator_comma = "," if directive_index < len(directive_data) - 1 else ""
            print("    {:5} : {}{}".format(repr(directive), repr("".join(sorted(directive_data[directive]))), seperator_comma))
        print("}")


def main():

    default_database_filename = "oeis.sqlite3"

    parser = argparse.ArgumentParser(description="Get a list of all characters in the main file directives and b-files, and return their occurrence counts.")

    parser.add_argument("-f", dest="filename", type=str, default=default_database_filename, help="OEIS SQLite3 database (default: {})".format(default_database_filename))

    args = parser.parse_args()

    with setup_logging():
        process_database_entries(args.filename)


if __name__ == "__main__":
    main()
