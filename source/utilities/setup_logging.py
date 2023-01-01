#! /usr/bin/env -S python3 -B

import logging

logger = logging.getLogger(__name__)


class _MyFormatter(logging.Formatter):
    """A formatter that is identical to the default Formatter, except that it replaces the comma by a period in the time."""
    def formatTime(self, record, datefmt = None):
        s = logging.Formatter.formatTime(self, record, datefmt)
        return s.replace(",", ".")


class LoggingContextManager:

    def __init__(self, logfile_name = None, logfile_mode = None, fmt = None, level = None, noisy = None, logstream = None):

        # Handle defaults

        if logfile_name is None:
            logfile_name = None

        if logfile_mode is None:
            logfile_mode = 'w'

        if fmt is None:
            fmt = "%(asctime)-23s | %(levelname)-8s | %(message)s"

        if level is None:
            level = logging.DEBUG

        if noisy is None:
            noisy = True

        if logstream is None:
            logstream = "stdout"

        # Store parameters

        self.logfile_name = logfile_name
        self.logfile_mode = logfile_mode
        self.fmt          = fmt
        self.level        = level
        self.noisy        = noisy
        self.logstream    = logstream

    def __enter__(self):

        # Add "PROGRESS" log level.

        logging.PROGRESS = logging.DEBUG + 5
        logging.addLevelName(logging.PROGRESS, "PROGRESS")

        # Set up log message handlers.

        handlers = []

        if self.logfile_name is not None:
            import datetime
            filename = datetime.datetime.now().strftime(self.logfile_name)
            handler = logging.FileHandler(filename, self.logfile_mode)
            handlers.append(handler)

        if self.logstream in ["stdout", "stderr"]:
            import sys
            handler = logging.StreamHandler(sys.stdout if self.logstream == "stdout" else sys.stderr)
            handlers.append(handler)

        root = logging.getLogger()

        if len(handlers) > 0:

            formatter = _MyFormatter(fmt = self.fmt)

            for handler in handlers:
                assert handler.formatter is None
                handler.setFormatter(formatter)
                root.addHandler(handler)

        root.setLevel(self.level)

        if self.noisy:
            logger.info("Logging started.")

    def __exit__(self, exc_type, exc_value, traceback):

        if self.noisy:
            logger.info("Logging stopped.")

        logging.shutdown()


def setup_logging(*args, **kwargs):
    """This function returns a context manager that encapsulates proper initialization and teardown of logging."""
    return LoggingContextManager(*args, **kwargs)


def _test_logging():

    logger.debug("This is a debug message.")
    logger.log(logging.PROGRESS, "This is a progress message.")
    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.critical("This is a critical message.")


def main():

    with setup_logging("test_%Y%m%d_%H%M%S.log", noisy = True):
        _test_logging()


if __name__ == "__main__":
    main()
