OEIS Tools
==========

This repository contains scripts to download, process, and analyze data from the
Online Encyclopedia of Integer Sequences (OEIS), as hosted on http://www.oeis.org.

Overview
--------

Our tools handle OEIS data from three sources:

1. remote: the OEIS database residing on the oeis.org server. The remote database can be accessed via HTTP or HTTPS requests.
2. local_sqlite3: a replica of the remote OEIS database as a local SQLite3 database.
3. local_pickle: a "pickled" version of the OEIS database, used for local analysis.

The local_sqlite3 database is obtained from the remote database by an automatic web-crawler called 'fetch_oeis_database.py'.
It contains the sequence metadata in the internal format as it is used on the remote side, as well as so called 'b-file'
data that contain sequence data a(n) up to high values of (n).

The local_pickle database is obtained from the local_sqlite3 database by parsing the data and turning it into OeisEntry
instances. The pickled list of all OeisEntry instances can be read in its entirety within a few seconds.

Dependencies
------------

- All code is written in Python 3.
- Some code depends on the 'numpy' library:
  - show_database_time.py
  - solve_linear_sequence.py
- Some code depends on the 'matplotlib' library:
  - show_database_time.py

Description of files
--------------------

filename                          |  description
----------------------------------|-------------------------------------------------------------------------------------------------------
README.md                         |  Text file (markdown) description of the project.
fetch_oeis_database.py            |  Python script to fetch and refresh data from the remote OEIS database to a local_sqlite3 database.
show_database_time.py             |  Python script to visualize time stamps in a given local_sqlite OEIS database.
parse_oeis_database.py            |  Python script to process the local_sqlite3 version of the OEIS database into a local_pickle.
find_sequences.py                 |  Python script to probe a local_pickle OEIS database for a given sequence (work in progress).
pickle_to_json.py                 |  Python script to process a local_pickle OEIS database to JSON.
solve_linear_sequence.py          |  Python script that finds linear sequences in a local_pickle database.
verify_oeis_catalog.py            |  Python script to verify our vetted OEIS sequences.
fraction_based_linear_algebra.py  |  Python module to perform matrix inversion without loss of precision using the Fraction type.
charmap.py                        |  Python module that defines lists of acceptable characters for the OEIS directives.
OeisEntry.py                      |  Python module that defines a simple class that contains (most of) the data of a single OEIS sequence.
TimerContextManager.py            |  Python module that simplifies timing lengthy operations using a context manager.

How it all fits together
------------------------

<img alt="Overview of OEIS tools" src="docs/oeis-tools.png" width="75%">