#! /usr/bin/env python3

import os
import sys
import sqlite3
import logging
import shutil
import json

from timer         import start_timer
from exit_scope    import close_when_done
from setup_logging import setup_logging

logger = logging.getLogger(__name__)


def process_database_entries(database_filename_in, dirname_out):

    if not os.path.exists(database_filename_in):
        logger.critical("Database file '{}' not found! Unable to continue.".format(database_filename_in))
        return

    (root, ext) = os.path.splitext(database_filename_in)

    if os.path.exists(dirname_out):
        logger.info("Removing stale directory '{}' ...".format(dirname_out))
        shutil.rmtree(dirname_out)

    # ========== fetch and process database entries, ordered by oeis_id.

    BATCH_SIZE = 1000

    with start_timer() as timer:
        with close_when_done(sqlite3.connect(database_filename_in)) as dbconn_in, close_when_done(dbconn_in.cursor()) as dbcursor_in:

            dbcursor_in.execute("SELECT oeis_id, t1, t2, main_content, bfile_content FROM oeis_entries ORDER BY oeis_id;")

            while True:

                oeis_entries = dbcursor_in.fetchmany(BATCH_SIZE)
                if len(oeis_entries) == 0:
                    break

                logger.log(logging.PROGRESS, "Processing OEIS entries A{:06} to A{:06} ...".format(oeis_entries[0][0], oeis_entries[-1][0]))

                for (oeis_id, t1, t2, main_content, bfile_content) in oeis_entries:

                    directory = os.path.join(dirname_out, "A{:03d}xxx".format(oeis_id // 1000), "A{:06d}".format(oeis_id))
                    os.makedirs(directory)

                    with open(os.path.join(directory, "metadata.json"), "w") as f:
                        metadata = [oeis_id, t1, t2]
                        json.dump(metadata, f)

                    with open(os.path.join(directory, "main_content.txt"), "w") as f:
                        f.write(main_content)

                    with open(os.path.join(directory, "bfile_content.txt"), "w") as f:
                        f.write(bfile_content)

        logger.info("Processed all database entries in {}.".format(timer.duration_string()))


def main():

    if len(sys.argv) != 2:
        print("Please specify the name of an OEIS database in Sqlite3 format.")
        return

    database_filename_in = sys.argv[1]

    (root, ext) = os.path.splitext(os.path.basename(database_filename_in))

    dirname_out = os.path.join("data", root + "_directory")

    with setup_logging(None):
        process_database_entries(database_filename_in, dirname_out)


if __name__ == "__main__":
    main()
