#! /usr/bin/env python3

"""Code to process raw OEIS entries, by parsing them."""

import os
import sys
import logging
import sqlite3
import concurrent.futures

from oeis_entry import parse_oeis_entry
from timer import start_timer
from exit_scope import close_when_done
from setup_logging import setup_logging

logger = logging.getLogger(__name__)


def create_database_schema(conn):
    """Ensure that the 'oeis_entries' table is present in the database."""

    schema = """
             CREATE TABLE IF NOT EXISTS oeis_entries (
                 oeis_id               INTEGER  PRIMARY KEY NOT NULL, -- OEIS ID number.
                 identification        TEXT,
                 value_list            TEXT     NOT NULL,
                 name                  TEXT     NOT NULL,
                 comments              TEXT,
                 detailed_references   TEXT,
                 links                 TEXT,
                 formulas              TEXT,
                 examples              TEXT,
                 maple_programs        TEXT,
                 mathematica_programs  TEXT,
                 other_programs        TEXT,
                 cross_references      TEXT,
                 keywords              TEXT     NOT NULL,
                 offset_a              INTEGER,
                 offset_b              INTEGER,
                 author                TEXT,
                 extensions_and_errors TEXT
             );
             """

    # Remove superfluous whitespace in SQL statement.
    schema = "\n".join(line[13:] for line in schema.split("\n"))[1:-1]

    # Execute the schema creation statement.

    conn.execute(schema)


def process_oeis_entry(oeis_entry):

    (oeis_id, main_content, bfile_content) = oeis_entry

    parsed_entry = parse_oeis_entry(oeis_id, main_content, bfile_content)

    result = (
        parsed_entry.oeis_id,
        parsed_entry.identification,
        ",".join(str(value) for value in parsed_entry.values),
        parsed_entry.name,
        parsed_entry.comments,
        parsed_entry.detailed_references,
        parsed_entry.links,
        parsed_entry.formulas,
        parsed_entry.examples,
        parsed_entry.maple_programs,
        parsed_entry.mathematica_programs,
        parsed_entry.other_programs,
        parsed_entry.cross_references,
        ",".join(str(keyword) for keyword in parsed_entry.keywords),
        parsed_entry.offset_a,
        parsed_entry.offset_b,
        parsed_entry.author,
        parsed_entry.extensions_and_errors
    )

    return result


def process_database_entries(database_filename_in):

    if not os.path.exists(database_filename_in):
        logger.critical("Database file '{}' not found! Unable to continue.".format(database_filename_in))
        return

    (root, ext) = os.path.splitext(database_filename_in)

    database_filename_out = root + "_parsed" + ext

    if os.path.exists(database_filename_out):
        logger.info("Removing stale file '{}' ...".format(database_filename_out))
        os.remove(database_filename_out)

    # ========== fetch and process database entries, ordered by oeis_id.

    BATCH_SIZE = 1000

    with start_timer() as timer:
        with close_when_done(sqlite3.connect(database_filename_in)) as dbconn_in, close_when_done(dbconn_in.cursor()) as dbcursor_in:
            with close_when_done(sqlite3.connect(database_filename_out)) as dbconn_out, close_when_done(dbconn_out.cursor()) as dbcursor_out:

                create_database_schema(dbconn_out)

                with concurrent.futures.ProcessPoolExecutor() as pool:

                    dbcursor_in.execute("SELECT oeis_id, main_content, bfile_content FROM oeis_entries ORDER BY oeis_id;")

                    while True:

                        oeis_entries = dbcursor_in.fetchmany(BATCH_SIZE)
                        if len(oeis_entries) == 0:
                            break

                        logger.log(logging.PROGRESS, "Processing OEIS entries A{:06} to A{:06} ...".format(oeis_entries[0][0], oeis_entries[-1][0]))

                        query = "INSERT INTO oeis_entries(oeis_id, identification, value_list, name, comments, detailed_references, links, formulas, examples, maple_programs, mathematica_programs, other_programs, cross_references, keywords, offset_a, offset_b, author, extensions_and_errors) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

                        dbcursor_out.executemany(query, pool.map(process_oeis_entry, oeis_entries))

                        dbconn_out.commit()

        logger.info("Processed all database entries in {}.".format(timer.duration_string()))


def main():

    if len(sys.argv) != 2:
        print("Please specify the name of an OEIS database in Sqlite3 format.")
        return

    database_filename_in = sys.argv[1]

    (root, ext) = os.path.splitext(database_filename_in)
    logfile = root + "_parsed.log"

    with setup_logging(logfile):
        process_database_entries(database_filename_in)


if __name__ == "__main__":
    main()
