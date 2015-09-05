#! /usr/bin/env python3

import sqlite3
from bs4 import BeautifulSoup, NavigableString

database_filename = "fetch_oeis_internal.sqlite3"

dbconn = sqlite3.connect(database_filename)
dbcursor = dbconn.cursor()

query = "SELECT oeis_id, fetch_node, fetch_url, fetch_timestamp, fetch_duration, fetch_html FROM fetched_oeis_entries LIMIT 1;"

dbcursor.execute(query)

while True:
    entry = dbcursor.fetchone()
    if entry is None:
        break
    (oeis_id, fetch_node, fetch_url, fetch_timestamp, fetch_duration, fetch_html) = entry
