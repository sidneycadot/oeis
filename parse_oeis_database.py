#! /usr/bin/env python3

# Code to analyze OEIS entries.

import sqlite3
import collections
import time
import logging
import pickle
import re

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

# A219022 has some arabic in its %N directive:
ASCII = " !\"#$%&'()*+,\-./0-9:;<=>?@A-Z[\\\\\]^_`a-z{|}~"
EXTENDED_LETTERS = "íéńàúøőöåäáüωÃÁîŜèóσ"
EXTENDED_SYMBOLS = "…∈≤≥•·°º×’´⌈⌉"
LIGATURES = "ﬂﬁﬀ"
A219022_ARABIC = "Āṭ" + "आर्यभट"
#XCODES = "\xa0\xad"

name_pattern = re.compile("[" + ASCII + EXTENDED_LETTERS + EXTENDED_SYMBOLS + LIGATURES + A219022_ARABIC + "]+$")

expected_keywords = [
    "base",  # dependent on base used for sequence
    "bref",  # sequence is too short to do any analysis with
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
    "less",  # reluctantly accepted
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
    # The following keywords occur often but are not documented:
    "changed",
    "look",
    "hear",
    "allocated"
]

expected_keywords_set = frozenset(expected_keywords)

class OeisEntry:
    def __init__(self, oeis_id, identification, numbers, name, offset, keywords):
        self.oeis_id        = oeis_id
        self.identification = identification
        self.numbers        = numbers
        self.name           = name
        self.offset         = offset
        self.keywords       = keywords
    def __str__(self):
        return "A{:06d}".format(self.oeis_id)

def parse_oeis_content(oeis_id, content):

    # ========== check order of directives

    lines = content.split("\n")

    for line in lines:
        assert len(line) >= 2
        assert line[0] == "%"

    directive_order = "".join(line[1] for line in lines)

    assert expected_directive_order.match(directive_order)

    # ========== collect directives

    lineI = None
    lineS = None
    lineT = None
    lineU = None
    lineN = None
    lineK = None
    lineO = None
    lineA = []

    for line in lines:

        directive = line[:2]

        assert directive in expected_directives

        if directive == "%I":
            assert lineI is None # only one %I directive is allowed
            lineI = line
        if directive == "%S":
            assert lineS is None # only one %S directive is allowed
            lineS = line
        elif directive == "%T":
            assert lineT is None # only one %T directive is allowed
            lineT = line
        elif directive == "%U":
            assert lineU is None # only one %U directive is allowed
            lineU = line
        if directive == "%N":
            assert lineN is None # only one %N directive is allowed
            lineN = line
        elif directive == "%K":
            assert lineK is None # only one %K directive is allowed
            lineK = line
        elif directive == "%O":
            assert lineO is None # only one %O directive is allowed
            lineO = line
        elif directive == "%A":
            lineA.append(line) # multiple %A directives are allowed

    # ========== process I directive

    assert (lineI is not None)

    if lineI == "%I":
        identification = None
    else:
        assert lineI.startswith("%I ")
        identification = lineI[3:]

        for identification_pattern in identification_patterns:
            if identification_pattern.match(identification) is not None:
                break
        else:
            logger.warning("[A{:06}] ill-formatted %I directive: '{}'".format(oeis_id, lineI))

    # ========== process S/T/U directives

    # An S line is mandatory.
    # If a T/U line is present, the previous line should be present and end in a comma, and vice versa.

    assert (lineS is not None)
    assert (lineT is not None) == (lineS is not None and lineS.endswith(","))
    assert (lineU is not None) == (lineT is not None and lineT.endswith(","))

    # Synthesize numbers

    assert (lineS is None) or lineS.startswith("%S ")
    assert (lineT is None) or lineT.startswith("%T ")
    assert (lineU is None) or lineU.startswith("%U ")

    S = "" if lineS is None else lineS[3:]
    T = "" if lineT is None else lineT[3:]
    U = "" if lineU is None else lineU[3:]

    STU = S + T + U

    numbers = [int(num_string) for num_string in STU.split(",") if len(num_string) > 0]

    assert ",".join([str(n) for n in numbers]) == STU

    # ========== process N directive

    assert (lineN is not None)
    assert lineN.startswith("%N ")

    name = lineN[3:]

    if name_pattern.match(name) is None:
        logger.warning("[A{:06}] bad characters in %N directive: {!r}".format(oeis_id, lineN))
        #zz = set(name) - set(" 0123456789<=>ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
        #zz = sorted(zz)
        #zz = ", ".join(["{!r}".format(z) for z in zz])
        #print("@@@", zz)
        #assert False

    # ========== process A directive

    if len(lineA) == 0:
        logger.warning("[A{:06}] missing %A directive".format(oeis_id))

    # ========== process O directive

    if lineO is None:
        logger.warning("[A{:06}] missing %O directive".format(oeis_id))
        offset = None
    else:
        assert lineO.startswith("%O ")
        offset = lineO[3:]

        offset = tuple(int(o) for o in offset.split(","))
        if len(offset) != 2:
            logger.warning("[A{:06}] ill-formatted %O directive: {!r}".format(oeis_id, lineO))
            pass

    # ========== process K directive

    assert (lineK is not None) and lineK.startswith("%K ")
    keywords = lineK[3:]

    keywords = keywords.split(",")

    # Check for unexpected keywords

    unexpected_keywords = set(keywords) - expected_keywords_set

    for unexpected_keyword in sorted(unexpected_keywords):
        if unexpected_keyword == "":
            logger.warning("[A{:06}] unexpected empty keyword in %K directive: {!r}".format(oeis_id, lineK))
        else:
            logger.warning("[A{:06}] unexpected keyword '{}' in %K directive: {!r}".format(oeis_id, unexpected_keyword, lineK))

    # Check for duplicate keywords

    keyword_counter = collections.Counter(keywords)
    for (keyword, count) in keyword_counter.items():
        if count > 1:
            logger.warning("[A{:06}] keyword '{}' occurs {} times in %K directive: {!r}".format(oeis_id, keyword, count, lineK))

    # Canonify keywords: remove empty keywords and duplicates, and sort.

    keywords = sorted(set(k for k in keywords if k != ""))

    # ========== return parsed values

    return OeisEntry(oeis_id, identification, numbers, name, offset, keywords)

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

    # ========== write pickled version.

    with open("oeis.pickle", "wb") as f:
        pickle.dump(entries, f)

if __name__ == "__main__":
    main()
