#! /usr/bin/env python3

# Code to analyze OEIS entries.

import sqlite3
import collections
import time
import logging
import pickle
import re

from OeisEntry import OeisEntry
from charmap import acceptable_characters, nasty

logger = logging.getLogger(__name__)

# As described here: https://oeis.org/eishelp1.html

# The directives %S, %T, and %U were originally intended as the absolute values of the sequence entries,
# with the signed entries given as %V, %W, %X. However, in the versions we downloaded, %S, %T, and %U
# are signed, and %V, %W, and %X are never present.

expected_directives = [
    "%I", # Identification Line                                     (REQUIRED)
    "%S", # Beginning f the sequence (line 1 of 3)                  (REQUIRED)
    "%T", # Beginning f the sequence (line 2 of 3)
    "%U", # Beginning f the sequence (line 3 of 3)
    "%N", # Name of sequence                                        (REQUIRED)
    "%D", # Detailed references
    "%H", # Links related to this sequence
    "%F", # Formula
    "%Y", # Cross-references to other sequences
    "%A", # Author, submitter, or other Authority                   (REQUIRED)
    "%O", # Offset a, b                                             (REQUIRED)
    "%p", # Computer program to produce the sequence (Maple)
    "%t", # Computer program to produce the sequence (Mathematica)
    "%o", # Computer program to produce the sequence (other computer language)
    "%E", # Extensions and errors
    "%e", # Examples
    "%K", # Keywords                                                (REQUIRED)
    "%C"  # Comments
]

# The order of expected directives, for any given entry is as follows:
#
# - First, a single "%I" entry.
# - Next, either a single "%S" line, an "%S" line followed by a "%T" line, or an "%S" line followed by a "%T" line followed by a "%U" line.
# - Next, a single "%N" line.
# - Next, zero or more "%C" lines.
# - Next, zero or more "%D" lines.
# - Next, zero or more "%H" lines.
# - Next, zero or more "%F" lines.
# - Next, zero or more "%e" lines.
# - Next, zero or more "%p" lines.
# - Next, zero or more "%t" lines.
# - Next, zero or more "%o" lines.
# - Next, zero or more "%Y" lines.
# - Next, a single "%K" line.
# - Next, an optional "%O" line.
# - Next, an optional "%A" line.
# - Next, zero or more "%E" lines.

expected_directive_order = re.compile("I(?:S|ST|STU)NC*D*H*F*e*p*t*o*Y*KO?A?E*$")

identification_patterns = [re.compile(pattern) for pattern in [
            "N[0-9]{4}$",
            "M[0-9]{4}$",
            "M[0-9]{4} N[0-9]{4}$",
            "M[0-9]{4} N[0-9]{4} N[0-9]{4}$"
        ]
    ]

# The expected keywords are documented in two places:
#
# https://oeis.org/eishelp1.html
# https://oeis.org/eishelp2.html
#
# The second documentation is more elaborate. It documents the keywords "changed",
# "hear", and "look", that the first page omits.

expected_keywords = [
    "base",  # dependent on base used for sequence
    "bref",  # sequence is too short to do any analysis with
    "changed",
    "cofr",  # a continued fraction expansion of a number
    "cons",  # a decimal expansion of a number
    "core",  # an important sequence
    "dead",  # an erroneous sequence
    "dumb",  # an unimportant sequence
    "dupe",  # duplicate of another sequence
    "easy",  # it is very easy to produce terms of sequence
    "eigen", # an eigensequence: a fixed sequence for some transformation
    "fini",  # a finite sequence
    "frac",  # numerators or denominators of sequence of rationals
    "full",  # the full sequence is given
    "hard",  # next term not known, may be hard to find. Would someone please extend this sequence?
    "hear",
    "less",  # reluctantly accepted
    "look",
    "more",  # more terms are needed! would someone please extend this sequence?
    "mult",  # multiplicative: a(mn)=a(m)a(n) if g.c.d.(m,n)=1
    "new",   # new (added within last two weeks, roughly)
    "nice",  # an exceptionally nice sequence
    "nonn",  # a sequence of nonnegative numbers
    "obsc",  # obscure, better description needed
    "sign",  # sequence contains negative numbers
    "tabf",  # An irregular (or funny-shaped) array of numbers made into a sequence by reading it row by row
    "tabl",  # typically a triangle of numbers, such as Pascal's triangle, made into a sequence by reading it row by row
    "uned",  # not edited
    "unkn",  # little is known; an unsolved problem; anyone who can find a formula or recurrence is urged to let me know.
    "walk",  # counts walks (or self-avoiding paths)
    "word",  # depends on words for the sequence in some language
    # The following keyword occurs often but is not documented:
    "allocated"
]

expected_keywords_set = frozenset(expected_keywords)

def parse_oeis_content(oeis_id, content):

    # ========== check order of directives

    lines = content.split("\n")

    for line in lines:
        assert len(line) >= 2
        assert line[0] == "%"

    directive_order = "".join(line[1] for line in lines)

    assert expected_directive_order.match(directive_order)

    # ========== collect directives

    line_I  = None
    line_S  = None
    line_T  = None
    line_U  = None
    line_N  = None
    lines_C = []
    lines_D = []
    lines_H = []
    line_K  = None
    line_O  = None
    lines_A = []

    for line in lines:

        directive = line[:2]
        assert directive in expected_directives

        if directive in acceptable_characters:
            unacceptable_characters = set(line) - acceptable_characters[directive]
            if unacceptable_characters:
                logger.warning("[A{:06}] unacceptable characters in directive {!r}: {}".format(oeis_id, line, ", ".join(["{!r}".format(c) for c in sorted(unacceptable_characters)])))

        if directive == "%I":
            assert line_I is None # only one %I directive is allowed
            line_I = line
        if directive == "%S":
            assert line_S is None # only one %S directive is allowed
            line_S = line
        elif directive == "%T":
            assert line_T is None # only one %T directive is allowed
            line_T = line
        elif directive == "%U":
            assert line_U is None # only one %U directive is allowed
            line_U = line
        if directive == "%N":
            assert line_N is None # only one %N directive is allowed
            line_N = line
        elif directive == "%C":
            lines_C.append(line) # multiple %C directives are allowed
        elif directive == "%D":
            lines_D.append(line) # multiple %D directives are allowed
        elif directive == "%H":
            lines_H.append(line) # multiple %H directives are allowed
        elif directive == "%K":
            assert line_K is None # only one %K directive is allowed
            line_K = line
        elif directive == "%O":
            assert line_O is None # only one %O directive is allowed
            line_O = line
        elif directive == "%A":
            lines_A.append(line) # multiple %A directives are allowed

    # ========== process I directive

    assert (line_I is not None)

    if line_I == "%I":
        identification = None
    else:
        assert line_I.startswith("%I ")
        identification = line_I[3:]

        for identification_pattern in identification_patterns:
            if identification_pattern.match(identification) is not None:
                break
        else:
            logger.warning("[A{:06}] ill-formatted %I directive: '{}'".format(oeis_id, line_I))

    # ========== process S/T/U directives

    # An S line is mandatory.
    # If a T/U line is present, the previous line should be present and end in a comma, and vice versa.

    assert (line_S is not None)
    assert (line_T is not None) == (line_S is not None and line_S.endswith(","))
    assert (line_U is not None) == (line_T is not None and line_T.endswith(","))

    # Synthesize numbers

    assert (line_S is None) or line_S.startswith("%S ")
    assert (line_T is None) or line_T.startswith("%T ")
    assert (line_U is None) or line_U.startswith("%U ")

    S = "" if line_S is None else line_S[3:]
    T = "" if line_T is None else line_T[3:]
    U = "" if line_U is None else line_U[3:]

    STU = S + T + U

    values = [int(value_string) for value_string in STU.split(",") if len(value_string) > 0]

    assert ",".join([str(n) for n in values]) == STU

    # ========== process N directive

    assert (line_N is not None)
    assert line_N.startswith("%N ")


    name = line_N[3:]

    # ========== process C directive

    for line_C in lines_C:

        assert line_C.startswith("%C ")
        comment = line_C[3:]

    # ========== process D directive

    for line_D in lines_D:
        assert line_D.startswith("%D ")
        detailed_reference = line_D[3:]

    # ========== process H directive

    for line_H in lines_H:
        assert line_H.startswith("%H ")
        link = line_H[3:]

    # ========== process A directive

    if len(lines_A) == 0:
        logger.warning("[A{:06}] missing %A directive".format(oeis_id))

    # ========== process O directive

    if line_O is None:
        logger.warning("[A{:06}] missing %O directive".format(oeis_id))
        offset = () # empty tuple
    else:
        assert line_O.startswith("%O ")
        offset = line_O[3:]

        offset = tuple(int(o) for o in offset.split(","))
        if len(offset) != 2:
            logger.warning("[A{:06}] ill-formatted %O directive: {!r}".format(oeis_id, line_O))
            pass

    # ========== process K directive

    assert (line_K is not None) and line_K.startswith("%K ")
    keywords = line_K[3:]

    keywords = keywords.split(",")

    # Check for unexpected keywords

    unexpected_keywords = set(keywords) - expected_keywords_set

    for unexpected_keyword in sorted(unexpected_keywords):
        if unexpected_keyword == "":
            logger.warning("[A{:06}] unexpected empty keyword in %K directive: {!r}".format(oeis_id, line_K))
        else:
            logger.warning("[A{:06}] unexpected keyword '{}' in %K directive: {!r}".format(oeis_id, unexpected_keyword, line_K))

    # Check for duplicate keywords

    keyword_counter = collections.Counter(keywords)
    for (keyword, count) in keyword_counter.items():
        if count > 1:
            logger.warning("[A{:06}] keyword '{}' occurs {} times in %K directive: {!r}".format(oeis_id, keyword, count, line_K))

    # Canonify keywords: remove empty keywords and duplicates, and sort.

    keywords = sorted(set(k for k in keywords if k != ""))

    if "full" in keywords and "fini" not in keywords:
        logger.warning("[A{:06}] has keyword 'full', but not 'fini'")
        sys.exit()

    # ========== return parsed values

    return OeisEntry(oeis_id, identification, values, name, offset, keywords)

def main():

    FORMAT = "%(asctime)-15s | %(levelname)-10s | %(message)s"
    logging.basicConfig(format = FORMAT, level = logging.DEBUG)

    # ========== fetch database entries, ordered by oeis_id.

    database_filename = "oeis.sqlite3"

    dbconn = sqlite3.connect(database_filename)
    try:
        dbcursor = dbconn.cursor()
        try:
            t1 = time.time()
            dbcursor.execute("SELECT oeis_id, content FROM oeis_entries ORDER BY oeis_id")
            oeis_data = dbcursor.fetchall()
            t2 = time.time()
            duration = (t2 - t1)
        finally:
            dbcursor.close()
    finally:
        dbconn.close()

    logger.info("Full fetch from database: {} entries in {:.3f} seconds".format(len(oeis_data), duration))

    # ========= parse database entries.

    entries = []
    for (oeis_id, oeis_content) in oeis_data:
        entry = parse_oeis_content(oeis_id, oeis_content)
        entries.append(entry)

    # ========== write pickled versions.

    with open("oeis.pickle", "wb") as f:
        pickle.dump(entries, f)

    with open("oeis-10000.pickle", "wb") as f:
        pickle.dump(entries[:10000], f)

if __name__ == "__main__":
    main()
