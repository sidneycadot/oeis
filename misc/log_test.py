#! /usr/bin/env python3

import sys
import logging
from exit_scope import shutdown_when_done

logger = logging.getLogger(__name__)

def test_logging():
    logger.debug("This is a debug message.")
    logger.log(logging.PROGRESS, "This is a progress message.")
    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.critical("This is a critical message.")

class MyFormatter(logging.Formatter):
    def formatTime(self, record, datefmt = None):
        s = logging.Formatter.formatTime(self, record, datefmt)
        return s.replace(",", ".")

def main():

    logging.PROGRESS = logging.DEBUG + 5
    logging.addLevelName(logging.PROGRESS, "PROGRESS")

    root = logging.getLogger()

    handlers = [
        logging.FileHandler('pipo.log', 'w'),
        logging.StreamHandler(sys.stdout)
    ]

    formatter = MyFormatter(fmt = "%(asctime)-25s | %(levelname)-8s | %(message)s")

    for handler in handlers:
        assert handler.formatter is None
        handler.setFormatter(formatter)
        root.addHandler(handler)

    root.setLevel(logging.DEBUG)

    with shutdown_when_done(logging):
        test_logging()

if __name__ == "__main__":
    main()
