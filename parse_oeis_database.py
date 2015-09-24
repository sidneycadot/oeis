#! /usr/bin/env python3

# Code to analyze OEIS entries.

import os
import sys
import re
import time
import pickle
import collections
import logging
import sqlite3

from OeisEntry import OeisEntry
from charmap   import acceptable_characters
from timer     import start_timer

logger = logging.getLogger(__name__)

expected_directive_order = re.compile("I(?:S|ST|STU)(?:|V|VW|VWX)NC*D*H*F*e*p*t*o*Y*KO?A?E*$")

identification_patterns = [re.compile(pattern) for pattern in [
            "N[0-9]{4}$",
            "M[0-9]{4}$",
            "M[0-9]{4} M[0-9]{4}$",
            "M[0-9]{4} N[0-9]{4}$",
            "M[0-9]{4} N[0-9]{4} N[0-9]{4}$"
        ]
    ]

# The expected keywords are documented in three places:
#
# http://oeis.org/eishelp1.html
# http://oeis.org/eishelp2.html                                (more elaborate than the first; documents the keywords "changed", "hear", and "look").
# http://oeis.org/wiki/User:Charles_R_Greathouse_IV/Keywords   (more up-to-date).

expected_keywords = [
    "allocated",  #
    "allocating", #
    "base",       # dependent on base used for sequence
    "bref",       # sequence is too short to do any analysis with
    "changed",    #
    "cofr",       # a continued fraction expansion of a number
    "cons",       # a decimal expansion of a number
    "core",       # an important sequence
    "dead",       # an erroneous sequence
    "dumb",       # an unimportant sequence
    "dupe",       # duplicate of another sequence
    "easy",       # it is very easy to produce terms of sequence
    "eigen",      # an eigensequence: a fixed sequence for some transformation
    "fini",       # a finite sequence
    "frac",       # numerators or denominators of sequence of rationals
    "full",       # the full sequence is given
    "hard",       # next term not known, may be hard to find. Would someone please extend this sequence?
    "hear",       #
    "less",       # reluctantly accepted
    "look",       #
    "more",       # more terms are needed! would someone please extend this sequence?
    "mult",       # multiplicative: a(mn)=a(m)a(n) if g.c.d.(m,n)=1
    "new",        # new (added within last two weeks, roughly)
    "nice",       # an exceptionally nice sequence
    "nonn",       # a sequence of nonnegative numbers
    "obsc",       # obscure, better description needed
    "probation",  #
    "recycled",   #
    "sign",       # sequence contains negative numbers
    "tabf",       # An irregular (or funny-shaped) array of numbers made into a sequence by reading it row by row
    "tabl",       # typically a triangle of numbers, such as Pascal's triangle, made into a sequence by reading it row by row
    "uned",       # not edited
    "unkn",       # little is known; an unsolved problem; anyone who can find a formula or recurrence is urged to let me know.
    "walk",       # counts walks (or self-avoiding paths)
    "word"        # depends on words for the sequence in some language
]

expected_keywords_set = frozenset(expected_keywords)

bfile_line_pattern = re.compile("(-?[0-9]+)[ \t]+(-?[0-9]+)")

def parse_main_content(oeis_id, main_content):

    # As described here: https://oeis.org/eishelp1.html

    # The directives %S, %T, and %U were originally intended as the absolute values of the sequence entries,
    # with the signed entries given as %V, %W, %X. However, in the versions we downloaded, %S, %T, and %U
    # are signed, and %V, %W, and %X are never present.

    #expected_directives = [
    #    "%I", # Identification Line                                     (REQUIRED)
    #    "%S", # Beginning f the sequence (line 1 of 3)                  (REQUIRED)
    #    "%T", # Beginning f the sequence (line 2 of 3)
    #    "%U", # Beginning f the sequence (line 3 of 3)
    #    "%N", # Name of sequence                                        (REQUIRED)
    #    "%D", # Detailed references
    #    "%H", # Links related to this sequence
    #    "%F", # Formula
    #    "%Y", # Cross-references to other sequences
    #    "%A", # Author, submitter, or other Authority                   (REQUIRED)
    #    "%O", # Offset a, b                                             (REQUIRED)
    #    "%p", # Computer program to produce the sequence (Maple)
    #    "%t", # Computer program to produce the sequence (Mathematica)
    #    "%o", # Computer program to produce the sequence (other computer language)
    #    "%E", # Extensions and errors
    #    "%e", # Examples
    #    "%K", # Keywords                                                (REQUIRED)
    #    "%C"  # Comments
    #]

    # The order of expected directives, for any given entry is as follows:
    #
    # - First, a single "%I" entry.
    # - Next, either a single "%S" line, an "%S" line followed by a "%T" line, or an "%S" line followed by a "%T" line followed by a "%U" line.
    # - Next, either a single "%V" line, a "%V" line followed by a "%W" line, or a "%V" line followed by a "%W" line followed by an "%X" line.
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

    # Select only lines that have the proper directive format.

    directive_line_pattern = "%(.) A{:06d}(.*)$".format(oeis_id)

    lines = re.findall(directive_line_pattern, main_content, re.MULTILINE)

    # remove space between Axxxxxx and value

    new_lines = []
    for (directive, directive_value) in lines:
        if directive_value != "":
            if directive_value.startswith(" "):
                directive_value = directive_value[1:]
            else:
                logger.warning("[A{:06}] (P16) The %{} directive should have a space before the start of its value.".format(oeis_id, directive))

        new_lines.append((directive, directive_value))

    lines = new_lines
    del new_lines

    if True: # check format

        header = "# Greetings from The On-Line Encyclopedia of Integer Sequences! http://oeis.org/\n\nSearch: id:a{:06d}\nShowing 1-1 of 1\n\n".format(oeis_id)

        check_main_content = ["%{} A{:06d}{}{}\n".format(directive, oeis_id, "" if directive_value == "" else " ", directive_value) for (directive, directive_value) in lines]

        footer = "\n# Content is available under The OEIS End-User License Agreement: http://oeis.org/LICENSE\n"

        check_main_content = header + "".join(check_main_content) + footer

        if main_content != check_main_content:
            logger.warning("[A{:06}] (P17) Main content reconstruction failed.".format(oeis_id))

    # ========== check order of directives

    directive_order = "".join(directive for (directive, directive_value) in lines)

    assert expected_directive_order.match(directive_order)

    # ========== collect directives

    line_I  = None
    line_S  = None
    line_T  = None
    line_U  = None
    line_V  = None
    line_W  = None
    line_X  = None
    line_N  = None
    lines_C = []
    lines_D = []
    lines_H = []
    line_K  = None
    line_O  = None
    lines_A = []

    for (directive, directive_value) in lines:

        if directive in acceptable_characters:
            unacceptable_characters = set(directive_value) - acceptable_characters[directive]
            if unacceptable_characters:
                logger.warning("[A{:06}] (P10) Unacceptable characters in value of %{} directive ({!r}): {}.".format(oeis_id, directive, directive_value, ", ".join(["{!r}".format(c) for c in sorted(unacceptable_characters)])))

        if directive == 'I':
            assert line_I is None # only one %I directive is allowed
            line_I = directive_value
        elif directive == 'S':
            assert line_S is None # only one %S directive is allowed
            line_S = directive_value
        elif directive == 'T':
            assert line_T is None # only one %T directive is allowed
            line_T = directive_value
        elif directive == 'U':
            assert line_U is None # only one %U directive is allowed
            line_U = directive_value
        if directive == 'V':
            assert line_V is None # only one %V directive is allowed
            line_V = directive_value
        elif directive == 'W':
            assert line_W is None # only one %W directive is allowed
            line_W = directive_value
        elif directive == 'X':
            assert line_X is None # only one %X directive is allowed
            line_X = directive_value
        elif directive == 'N':
            assert line_N is None # only one %N directive is allowed
            line_N = directive_value
        elif directive == 'C':
            lines_C.append(directive_value) # multiple %C directives are allowed
        elif directive == 'D':
            lines_D.append(directive_value) # multiple %D directives are allowed
        elif directive == 'H':
            lines_H.append(directive_value) # multiple %H directives are allowed
        elif directive == 'K':
            assert line_K is None # only one %K directive is allowed
            line_K = directive_value
        elif directive == 'O':
            assert line_O is None # only one %O directive is allowed
            line_O = directive_value
        elif directive == 'A':
            lines_A.append(directive_value) # multiple %A directives are allowed

    # ========== process I directive

    assert (line_I is not None)

    identification = line_I
    if identification == "":
        identification = None
    else:
        for identification_pattern in identification_patterns:
            if identification_pattern.match(identification) is not None:
                break
        else:
            logger.warning("[A{:06}] (P14) Unusual %I directive value: '{}'.".format(oeis_id, identification))

    # ========== process S/T/U directives

    # An S line is mandatory.
    # If a T/U line is present, the previous line should be present and end in a comma, and vice versa.

    assert (line_S is not None)
    assert (line_T is not None) == (line_S is not None and line_S.endswith(","))
    assert (line_U is not None) == (line_T is not None and line_T.endswith(","))

    if line_S == "":
        logger.warning("[A{:06}] (P3) Unusual %S directive without value.".format(oeis_id))

    S = "" if line_S is None else line_S
    T = "" if line_T is None else line_T
    U = "" if line_U is None else line_U

    STU = S + T + U

    stu_values = [int(value_string) for value_string in STU.split(",") if len(value_string) > 0]

    assert ",".join([str(stu_value) for stu_value in stu_values]) == STU

    assert all(stu_value >= 0 for stu_value in stu_values)

    # ========== process V/W/X directives

    assert (line_W is not None) == (line_V is not None and line_V.endswith(","))
    assert (line_X is not None) == (line_W is not None and line_W.endswith(","))

    V = "" if line_V is None else line_V
    W = "" if line_W is None else line_W
    X = "" if line_X is None else line_X

    VWX = V + W + X

    vwx_values = [int(value_string) for value_string in VWX.split(",") if len(value_string) > 0]

    assert ",".join([str(vwx_value) for vwx_value in vwx_values]) == VWX

    # ==========

    if line_V is not None:

        assert any(vwx_value < 0 for vwx_value in vwx_values)
        assert (abs(vwx_value) == stu_value for (vwx_value, stu_value) in zip(vwx_values, stu_values))

        assert [abs(v) for v in vwx_values] == stu_values

        main_values = vwx_values
    else:
        main_values = stu_values

    # ========== process N directive

    assert (line_N is not None)
    name = line_N

    # ========== process C directive
    # ========== process D directive
    # ========== process H directive

    # ========== process A directive

    if len(lines_A) == 0:
        logger.warning("[A{:06}] (P1) Missing %A directive.".format(oeis_id))

    # ========== process O directive

    if line_O is None:
        logger.warning("[A{:06}] (P2) Missing %O directive.".format(oeis_id))
        offset = () # empty tuple
    else:
        offset = line_O

        offset = tuple(int(o) for o in offset.split(","))
        if len(offset) != 2:
            logger.warning("[A{:06}] (P4) Unusual %O directive value only has a single number: {!r}.".format(oeis_id, line_O))

    # ========== process K directive

    assert line_K is not None
    keywords = line_K

    keywords = keywords.split(",")

    # Check for unexpected keywords.

    unexpected_keywords = set(keywords) - expected_keywords_set

    for unexpected_keyword in sorted(unexpected_keywords):
        if unexpected_keyword == "":
            logger.warning("[A{:06}] (P13) Unexpected empty keyword in %K directive value.".format(oeis_id))
        else:
            logger.warning("[A{:06}] (P15) Unexpected keyword '{}' in %K directive value.".format(oeis_id, unexpected_keyword))

    # Check for duplicate keywords.

    keyword_counter = collections.Counter(keywords)
    for (keyword, count) in keyword_counter.items():
        if count > 1:
            logger.warning("[A{:06}] (P11) Keyword '{}' occurs {} times in %K directive value: {!r}.".format(oeis_id, keyword, count, line_K))

    # Canonify keywords: remove empty keywords and duplicates.
    # We not sort.

    canon = []
    for keyword in keywords:
        if not(keyword == "" or keyword in canon):
            canon.append(keyword)

    keywords = canon
    del canon

    # Return the data parsed from the main_content.

    return (identification, main_values, name, offset, keywords)

def parse_bfile_content(oeis_id, bfile_content):

    lines = bfile_content.split("\n")

    indexes = []
    values = []

    for (line_nr, line) in enumerate(lines, 1):

        if line.startswith("#"):
            continue

        line = line.strip()

        if len(line) == 0:
            continue

        match = bfile_line_pattern.match(line)

        if match is None:
            logger.error("[A{:06}] (P12) b-file line {} cannot be parsed: '{}'; terminating parse.".format(oeis_id, line_nr, line))
            break

        index = int(match.group(1))
        value = int(match.group(2))

        if len(indexes) > 0 and (index != indexes[-1] + 1):
            logger.error("[A{:06}] (P8) b-file line {} has indexes that are non-sequential; {} follows {}; terminating parse.".format(oeis_id, line_nr, index, indexes[-1]))
            break

        indexes.append(index)

        values.append(value)

    assert len(indexes) == len(values)

    first_index = indexes[0] if len(indexes) > 0 else None

    return (first_index, values)

def parse_oeis_content(oeis_id, main_content, bfile_content):

    (identification, main_values, name, offset, keywords) = parse_main_content (oeis_id, main_content)
    (bfile_first_index, bfile_values)                     = parse_bfile_content(oeis_id, bfile_content)

    # Merge values obtained from S/T/U or V/W/X directives in main_content with the b-file values.

    if not len(bfile_values) >= len(main_values):
        logger.warning("[A{:06}] (P7) Main file has more values than b-file (main: {}, b-file: {}).".format(oeis_id, len(main_values), len(bfile_values)))

    if all(bfile_values[i] == main_values[i] for i in range(min(len(main_values), len(bfile_values)))):
        # The values are fully consistent.
        # Use the one that has the most entries.
        values = bfile_values if len(bfile_values) > len(main_values) else main_values
    else:
        logger.error("[A{:06}] (P5) Main/b-file values mismatch. Falling back on main values.".format(oeis_id))
        logger.info ("[A{:06}]     main values ........ : {}...".format(oeis_id, main_values[:10]))
        logger.info ("[A{:06}]     b-file values ...... : {}...".format(oeis_id, bfile_values[:10]))

        values = main_values  # Probably the safest choice.

    if (len(offset) > 0) and (offset[0] != bfile_first_index):
            logger.error("[A{:06}] (P6) %O directive claims first index is {}, but b-file starts at index {}.".format(oeis_id, offset[0], bfile_first_index))

    indexes_where_magnitude_exceeds_1 = [i for i in range(len(values)) if abs(values[i]) > 1]
    if len(indexes_where_magnitude_exceeds_1) > 0:
        first_index_where_magnitude_exceeds_1 = 1 + min(indexes_where_magnitude_exceeds_1)
    else:
        first_index_where_magnitude_exceeds_1 = 1

    if len(offset) > 1 and (offset[1] != first_index_where_magnitude_exceeds_1):
        logger.error("[A{:06}] (P9) %O directive claims first index where magnitude exceeds 1 is {}, but values suggest this should be {}.".format(oeis_id, offset[1], first_index_where_magnitude_exceeds_1))

    # Return parsed values.

    return OeisEntry(oeis_id, identification, values, name, offset, keywords)

def process_database(database_filename):

    if not os.path.exists(database_filename):
        logger.critical("Database file '{}' not found! Unable to continue.".format(database_filename))
        return

    # ========== fetch and process database entries, ordered by oeis_id.

    entries = []

    with start_timer() as timer:
        dbconn = sqlite3.connect(database_filename)
        try:
            dbcursor = dbconn.cursor()
            try:
                dbcursor.execute("SELECT oeis_id, main_content, bfile_content FROM oeis_entries ORDER BY oeis_id;")
                while True:
                    oeis_entry = dbcursor.fetchone()
                    if oeis_entry is None:
                        break
                    (oeis_id, main_content, bfile_content) = oeis_entry
                    if oeis_id % 10 == 0:
                        logger.log(logging.PROGRESS, "Processing [A{:06}] ...".format(oeis_id))
                    entry = parse_oeis_content(oeis_id, main_content, bfile_content)
                    entries.append(entry)
            finally:
                dbcursor.close()
        finally:
            dbconn.close()

        logger.info("Parsed {} entries in {}.".format(len(entries), timer.duration_string()))

    # ========== write pickled versions.

    (root, ext) = os.path.splitext(database_filename)

    logger.info("Writing pickle file ...")
    with start_timer() as timer:
        filename_pickle = os.path.join(root + ".pickle")
        with open(filename_pickle, "wb") as f:
            pickle.dump(entries, f)
        logger.info("Wrote all {} entries to '{}' in {}.".format(len(entries), filename_pickle, timer.duration_string()))

    WRITE_REDUCED_THRESHOLD = 10000

    if len(entries) > WRITE_REDUCED_THRESHOLD:
        logger.info("Writing reduced-size pickle file ...")
        reduced_entries = entries[:WRITE_REDUCED_THRESHOLD]
        with start_timer() as timer:
            filename_pickle_reduced = root + "-{}.pickle".format(len(reduced_entries))
            with open(filename_pickle_reduced, "wb") as f:
                pickle.dump(reduced_entries, f)

            logger.info("Wrote first {} entries to '{}' in {}.".format(len(reduced_entries), filename_pickle_reduced, timer.duration_string()))

def main():

    if len(sys.argv) != 2:
        print("Please specify the name of an OEIS database in Sqlite3 format.")
        return

    database_filename = sys.argv[1]

    logging.PROGRESS = logging.DEBUG + 5
    logging.addLevelName(logging.PROGRESS, "PROGRESS")

    FORMAT = "%(asctime)-15s | %(levelname)-8s | %(message)s"
    logging.basicConfig(format = FORMAT, level = logging.DEBUG)

    try:
        with start_timer() as timer:
            process_database(database_filename)
            logger.info("Database processing completed in {}.".format(timer.duration_string()))
    finally:
        logging.shutdown()

if __name__ == "__main__":
    main()
