#! /usr/bin/env python3

import sqlite3
import collections
import time
import logging
import pickle

logger = logging.getLogger(__name__)

# As described here: https://oeis.org/eishelp1.html
expected_keywords = [
    "base",
    "bref",
    "cofr",
    "cons",
    "core",
    "dead",
    "dumb",
    "dupe",
    "easy",
    "eigen",
    "fini",
    "frac",
    "full",
    "hard",
    "less",
    "more",
    "mult",
    "new",
    "nice",
    "nonn",
    "obsc",
    "sign",
    "tabf",
    "tabl",
    "uned",
    "unkn",
    "walk",
    "word",
    # The following keywords occur often but are not documented:
    "changed",
    "look",
    "hear",
    "allocated"
]

expected_keywords_set = frozenset(expected_keywords)

def parse_oeis_contents(oeis_id, oeis_contents):

    lines = oeis_contents.split("\n")

    lineS = None
    lineT = None
    lineU = None
    lineK = None
    lineO = None

    for line in lines:
        assert len(line) >= 2
        assert line[0] == "%"
        directive = line[1]
        if directive == "S":
            assert lineS is None
            lineS = line
        elif directive == "T":
            assert lineT is None
            lineT = line
        elif directive == "U":
            assert lineU is None
            lineU = line
        elif directive == "K":
            assert lineK is None
            lineK = line
        elif directive == "O":
            assert lineO is None
            lineO = line

    # Process S/T/U lines

    assert lineS is not None
    assert lineS.startswith("%S ")
    lineS = lineS[3:]

    assert not((lineT is None) and (lineU is not None))

    if lineT is not None:
        assert lineT.startswith("%T ")
        lineT = lineT[3:]
        assert lineS.endswith(",")

    if lineU is not None:
        assert lineU.startswith("%U ")
        lineU = lineU[3:]
        assert lineT.endswith(",")

    STU = lineS
    if lineT is not None:
        STU += lineT
    if lineU is not None:
        STU += lineU

    if len(STU) == 0:
        numbers = []
    else:
        numbers = [int(n) for n in STU.split(",")]

    assert ",".join([str(n) for n in numbers]) == STU

    # Process K line

    assert lineK is not None
    assert lineK.startswith("%K ")
    lineK = lineK[3:]

    keywords = lineK.split(",")

    unexpected_keywords = sorted(set(keywords) - expected_keywords_set)
    for unexpected_keyword in unexpected_keywords:
        if unexpected_keyword == "":
            logger.warning("[A{:06}] unexpected empty keyword in %K directive: '{}'".format(oeis_id, lineK))
        else:
            logger.warning("[A{:06}] unexpected keyword '{}' in %K directive: '{}'".format(oeis_id, unexpected_keyword, lineK))

    # canonify keywords
    keywords = sorted(set(k for k in keywords if k != ""))

    keyword_counter = collections.Counter(keywords)
    for (keyword, count) in keyword_counter.items():
        if count > 1:
            logger.warning("[A{:06}] keyword '{}' occurs {} times in %K directive: '{}'".format(oeis_id, keyword, count, lineK))


    # Process O line

    if lineO is None:
        #logger.warning("[A{:06}] missing %O directive".format(oeis_id))
        offset = None
    else:
        assert lineO.startswith("%O ")
        lineO = lineO[3:]

        offset = tuple(int(o) for o in lineO.split(","))
        if len(offset) != 2:
            logger.warning("[A{:06}] ill-formatted %O directive: '{}'".format(oeis_id, lineO))
            pass

    return (offset, keywords, numbers)

def main():

    FORMAT = "%(asctime)-15s | %(levelname)-10s | %(message)s"
    logging.basicConfig(format = FORMAT, level = logging.DEBUG)

    database_filename = "fetch_oeis_internal.sqlite3"

    dbconn = sqlite3.connect(database_filename)
    dbcursor = dbconn.cursor()

    t1 = time.time()
    dbcursor.execute("SELECT oeis_id, contents FROM fetched_oeis_entries;")
    oeis_data = dbcursor.fetchall()
    t2 = time.time()
    duration = (t2 - t1)
    logger.info("Full fetch from database: {} entries in {:.3f} seconds".format(len(oeis_data), duration))

    t1 = time.time()
    oeis_data.sort() # sort by OEIS ID
    t2 = time.time()
    duration = (t2 - t1)
    logger.info("Sort by OEIS-ID took {:.3f} seconds.".format(duration))

    entries = []

    for (oeis_id, oeis_contents) in oeis_data:
        (offset, keywords, numbers) = parse_oeis_contents(oeis_id, oeis_contents)
        entries.append((oeis_id, offset, keywords, numbers))

    with open("oeis.pickle", "wb") as f:
        pickle.dump(entries, f)

if __name__ == "__main__":
    main()
