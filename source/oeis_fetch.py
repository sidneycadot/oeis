#! /usr/bin/env -S python3 -B

"""Fetch OEIS entry from remote database and write it locally."""

import argparse
import logging
from typing import List

from utilities.setup_logging import setup_logging
from utilities.fetch_remote_oeis_entry import fetch_remote_oeis_entry

logger = logging.getLogger(__name__)


def fetch_oeis_entries(oeis_ids: List[int]) -> None:

        for oeis_id in oeis_ids:
            logger.info("Fetching entry {:d} ...".format(oeis_id))
            entry = fetch_remote_oeis_entry(oeis_id, True)
            logger.info("Writing a{:06d}_remote.txt ...".format(oeis_id))
            with open("a{:06}_remote.txt".format(oeis_id), "w", encoding='utf-8') as fo:
                fo.write(entry.main_content)
            logger.info("Writing b{:06d}_remote.txt ...".format(oeis_id))
            with open("b{:06}_remote.txt".format(oeis_id), "w", encoding='utf-8') as fo:
                fo.write(entry.bfile_content)

def main():

    parser = argparse.ArgumentParser(description="Fetch main file and b-file of selected OEIS entries from the OEIS server.")

    parser.add_argument("oeis_ids", metavar="oeis_id", nargs="+", type=int, help="OEIS ID to be extracted")

    args = parser.parse_args()

    with setup_logging():
        fetch_oeis_entries(args.oeis_ids)


if __name__ == "__main__":
    main()
