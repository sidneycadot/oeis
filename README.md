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

- The code is written in Python 3.
- Much of the code depends on the 'numpy' library.
- Some scripts use the 'matplotlib' library to produce graphs.

Description of files
--------------------

filename                               | description
---------------------------------------|---------------------------------------------
README.md                              | Description of the project (this file).
fetch_oeis_database.py                 | Python script to fetch and refresh data from the online OEIS to a local Sqlite3 database.
show_database_time.py                  | Python script to visualize time stamps in a given SQLite3 OEIS database.
parse_oeis_database.py                 | Python script to parse the SQLite3 version of the local OEIS database into a 'pickled' format.
charmap.py                             | Python module that defines lists of acceptable characters for the OEIS.
OeisEntry.py                           | A simple class that contains (most of) the data of a single OEIS sequence.
pickle_to_json.py                      | Python script to generate a JSON version of the pickled database.
fraction_based_linear_algebra.py       | Code to perform matrix inversion without loss of precision using the Fraction type.
solve_lineair_sequence.py              | Probe local OEIS 'pickled' database.
catalog.py                             | My own catalog of vetted OEIS sequences.
findsequences.py                       | Probe local OEIS 'pickled' database for a given sequence (work in progress).
