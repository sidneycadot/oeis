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
import concurrent.futures

from OeisEntry     import OeisEntry
from charmap       import acceptable_characters
from timer         import start_timer
from exit_scope    import close_when_done
from setup_logging import setup_logging

logger = logging.getLogger(__name__)

expected_directive_order = re.compile("I(?:S|ST|STU)(?:|V|VW|VWX)NC*D*H*F*e*p*t*o*Y*KO?A?E*$")

identification_pattern = re.compile("[MN][0-9]{4}( [MN][0-9]{4})*$")

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

def digits(n):
    return len(str(abs(n)))


def parse_optional_multiline_directive(dv, directive):
    if directive not in dv:
        return None
    else:
        return "".join(line + "\n" for line in dv[directive])


def parse_mandatory_singleline_directive(dv, directive):
    assert directive in dv
    assert len(dv[directive]) == 1
    return dv[directive][0]


def parse_optional_singleline_directive(dv, directive):
    if directive not in dv:
        return None
    else:
        assert len(dv[directive]) == 1
        return dv[directive][0]


def parse_value_directives(dv, directives):

    expect_next = 0 # -1 = no, 0 = maybe (first line), 1 = yes

    lines = []

    for directive in directives:
        if directive in dv:
            assert len(dv[directive]) == 1
            line = dv[directive][0]
            lines.append(line)
            if line.endswith(","):
                expect_next = 1
            else:
                expect_next = -1
        else:
            assert expect_next <= 0
            expect_next = -1

    assert expect_next == -1

    if len(lines) == 0:
        return None

    lines = "".join(lines)

    if lines == "":
        return []

    values = [int(value_string) for value_string in lines.split(",")]

    assert ",".join(str(value) for value in values) == lines
    return values


def check_keywords(oeis_id, keywords):

    # Check forbidden combinations of keywords.

    if "tabl" in keywords and "tabf" in keywords:
        logger.warning("A{:06} (P21) Keywords 'tabl' and 'tabf' occur together, which should not happen.".format(oeis_id))

    if "nice" in keywords and "less" in keywords:
        logger.warning("A{:06} (P22) Keywords 'nice' and 'less' occur together, which should not happen.".format(oeis_id))

    if "easy" in keywords and "hard" in keywords:
        logger.warning("A{:06} (P23) Keywords 'easy' and 'hard' occur together, which should not happen.".format(oeis_id))

    if "nonn" in keywords and "sign" in keywords:
        logger.warning("A{:06} (P24) Keywords 'nonn' and 'sign' occur together, which should not happen.".format(oeis_id))

    if "full" in keywords and "more" in keywords:
        logger.warning("A{:06} (P25) Keywords 'full' and 'more' occur together, which should not happen.".format(oeis_id))

    # Check exclusive keywords.

    if "allocated"  in keywords and len(keywords) > 1:
        logger.warning("A{:06} (P26) Keyword 'allocated' occurs in combination with other keywords, which should not happen.".format(oeis_id))

    if "allocating" in keywords and len(keywords) > 1:
        logger.warning("A{:06} (P27) Keyword 'allocating' occurs in combination with other keywords, which should not happen.".format(oeis_id))

    if "dead" in keywords and len(keywords) > 1:
        logger.warning("A{:06} (P28) Keyword 'dead' occurs in combination with other keywords, which should not happen.".format(oeis_id))

    if "recycled" in keywords and len(keywords) > 1:
        logger.warning("A{:06} (P29) Keyword 'recycled' occurs in combination with other keywords, which should not happen.".format(oeis_id))

    # Check presence of either 'none' or 'sign' keyword.

    if not(("allocated" in keywords) or ("allocating" in keywords) or ("dead" in keywords) or ("recycled" in keywords)):
        if ("nonn" not in keywords) and ("sign" not in keywords):
            logger.warning("A{:06} (P30) Keyword 'nonn' or 'sign' are both absent.".format(oeis_id))


def parse_main_content(oeis_id, main_content):

    # The order and count of expected directives, for any given entry, is as follows:
    #
    # * %I   one                Identification line.
    # * %STU S/ST/STU           Unsigned values.
    # * %VWX V/VW/VWX           Signed values.
    # * %N   one                Name.
    # - %C   zero or more       Comments.
    # - %D   zero or more       Detailed references.
    # - %H   zero or more       Links.
    # - %F   zero or more       Formula.
    # - %e   zero or more       Examples.
    # - %p   zero or more       Maple programs.
    # - %t   zero or more       Mathematica programs.
    # - %o   zero or more       Other programs.
    # - %Y   zero or more       Cross-references to other sequences.
    # * %K   one                Keywords.
    # * %O   zero or one        Offset a or a,b.
    # - %A   zero or one        Author, submitter, or authority.
    # - %E   zero or more       Extensions and errors.

    # Select only lines that have the proper directive format.

    directive_line_pattern = "%(.) A{:06}(.*)$".format(oeis_id)

    lines = re.findall(directive_line_pattern, main_content, re.MULTILINE)

    # remove space between Axxxxxx and value

    new_lines = []
    for (directive, directive_value) in lines:
        if directive_value != "":
            if directive_value.startswith(" "):
                directive_value = directive_value[1:]
                if directive_value == "":
                    logger.warning("[A{:06}] (P18) The %{} directive has a trailing space but no value.".format(oeis_id, directive))
            else:
                logger.warning("[A{:06}] (P16) The %{} directive should have a space before the start of its value.".format(oeis_id, directive))

        new_lines.append((directive, directive_value))

    lines = new_lines
    del new_lines

    if True: # check format

        header = "# Greetings from The On-Line Encyclopedia of Integer Sequences! http://oeis.org/\n\nSearch: id:a{:06}\nShowing 1-1 of 1\n\n".format(oeis_id)

        check_main_content = ["%{} A{:06}{}{}\n".format(directive, oeis_id, "" if directive_value == "" else " ", directive_value) for (directive, directive_value) in lines]

        footer = "\n# Content is available under The OEIS End-User License Agreement: http://oeis.org/LICENSE\n"

        check_main_content = header + "".join(check_main_content) + footer

        if main_content != check_main_content:
            logger.warning("[A{:06}] (P17) Main content reconstruction failed.".format(oeis_id))
            logger.info   ("[A{:06}]       original ............ : {!r}.".format(oeis_id, main_content))
            logger.info   ("[A{:06}]       reconstruction ...... : {!r}.".format(oeis_id, check_main_content))

    # ========== check order of directives

    directive_order = "".join(directive for (directive, directive_value) in lines)

    assert expected_directive_order.match(directive_order)

    # ========== collect directives

    dv = {}

    for (directive, value) in lines:

        if directive not in dv:
            dv[directive] = []

        dv[directive].append(value)

        if directive in acceptable_characters:
            unacceptable_characters = set(value) - acceptable_characters[directive]
            if unacceptable_characters:
                logger.warning("[A{:06}] (P10) Unacceptable characters in value of %{} directive ({!r}): {}.".format(oeis_id, directive, value, ", ".join(["{!r}".format(c) for c in sorted(unacceptable_characters)])))

    # ========== parse all directives

    identification        = parse_mandatory_singleline_directive (dv, 'I')
    stu_values            = parse_value_directives               (dv, "STU")
    vwx_values            = parse_value_directives               (dv, "VWX")
    name                  = parse_mandatory_singleline_directive (dv, 'N')
    comments              = parse_optional_multiline_directive   (dv, 'C')
    detailed_references   = parse_optional_multiline_directive   (dv, 'D')
    links                 = parse_optional_multiline_directive   (dv, 'H')
    formulas              = parse_optional_multiline_directive   (dv, 'F')
    examples              = parse_optional_multiline_directive   (dv, 'e')
    maple_programs        = parse_optional_multiline_directive   (dv, 'p')
    mathematica_programs  = parse_optional_multiline_directive   (dv, 't')
    other_programs        = parse_optional_multiline_directive   (dv, 'o')
    cross_references      = parse_optional_multiline_directive   (dv, 'Y')
    keywords              = parse_mandatory_singleline_directive (dv, 'K')
    offset                = parse_optional_singleline_directive  (dv, 'O')
    author                = parse_optional_singleline_directive  (dv, 'A')
    extensions_and_errors = parse_optional_multiline_directive   (dv, 'E')

    # ========== process %K directive

    # We parse the keywords first, they may influence the warnings.

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

    # Canonify keywords: remove empty keywords and duplicates. We do not sort, though.

    canonized_keywords = []
    for keyword in keywords:
        if not (keyword == "" or keyword in canonized_keywords):
            canonized_keywords.append(keyword)

    # ========== process %I directive

    if identification == "":
        identification = None
    else:
        if identification_pattern.match(identification) is None:
            logger.warning("[A{:06}] (P14) Unusual %I directive value: '{}'.".format(oeis_id, identification))

    # ========== process value directives (%S/%T/%U and %V/%W/%X)

    assert stu_values is not None

    if len(stu_values) == 0:
        logger.warning("[A{:06}] (P03) No values listed (empty %S directive).".format(oeis_id))

    # Merge STU values and VWX values (if the latter are present).

    if vwx_values is None:
        main_values = stu_values
    else:
        assert any(vwx_value < 0 for vwx_value in vwx_values)
        assert len(stu_values) == len(vwx_values)
        assert [abs(v) for v in vwx_values] == stu_values
        main_values = vwx_values

    if "dead" not in keywords and "sign" not in keywords and any(value < 0 for value in main_values):
        logger.warning("[A{:06}] (P19) negative values are present, but 'sign' keyword is missing.".format(oeis_id))

    if len(main_values) > 0:
        max_digits = max(digits(v) for v in main_values)
        if max_digits > 1000:
            logger.warning("[A{:06}] (P20) Sequence contains extremely large values (up to {} digits).".format(oeis_id, max_digits))

    # ========== process %A directive

    if author is None:
        logger.warning("[A{:06}] (P01) Missing %A directive.".format(oeis_id))

    # ========== process %O directive

    if offset is None:
        logger.warning("[A{:06}] (P02) Missing %O directive.".format(oeis_id))
        offset_a = None
        offset_b = None
    else:
        offset_values = [int(o) for o in offset.split(",")]
        assert len(offset_values) in [1, 2]
        if len(offset_values) == 1:
            logger.warning("[A{:06}] (P04) The %O directive value only has a single number ({}).".format(oeis_id, offset_values[0]))
            offset_a = offset_values[0]
            offset_b = None
        else:
            offset_a = offset_values[0]
            offset_b = offset_values[1]

    # Return the data parsed from the main_content.

    return (identification, main_values, name, comments, detailed_references, links, formulas, examples,
            maple_programs, mathematica_programs, other_programs, cross_references, canonized_keywords, offset_a, offset_b, author, extensions_and_errors)


def parse_bfile_content(oeis_id, bfile_content):

    lines = bfile_content.split("\n")

    indexes = []
    values  = []

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
            logger.error("[A{:06}] (P08) b-file line {} has indexes that are non-sequential; {} follows {}; terminating parse.".format(oeis_id, line_nr, index, indexes[-1]))
            break

        indexes.append(index)

        values.append(value)

    assert len(indexes) == len(values)

    first_index = indexes[0] if len(indexes) > 0 else None

    return (first_index, values)


def parse_oeis_content(oeis_id, main_content, bfile_content):

    (identification, main_values, name, comments, detailed_references, links, formulas, examples,
     maple_programs, mathematica_programs, other_programs, cross_references, keywords, offset_a, offset_b, author, extensions_and_errors) = \
        parse_main_content (oeis_id, main_content)

    (bfile_first_index, bfile_values) = parse_bfile_content(oeis_id, bfile_content)

    # Merge values obtained from S/T/U or V/W/X directives in main_content with the b-file values.

    if not len(bfile_values) >= len(main_values):
        logger.warning("[A{:06}] (P07) Main file has more values than b-file (main: {}, b-file: {}).".format(oeis_id, len(main_values), len(bfile_values)))

    if all(bfile_values[i] == main_values[i] for i in range(min(len(main_values), len(bfile_values)))):
        # The values are fully consistent.
        # Use the one that has the most entries.
        values = bfile_values if len(bfile_values) > len(main_values) else main_values
    else:
        logger.error("[A{:06}] (P05) Main/b-file values mismatch. Falling back on main values.".format(oeis_id))
        logger.info ("[A{:06}]       main values ........ : {}...".format(oeis_id, main_values[:10]))
        logger.info ("[A{:06}]       b-file values ...... : {}...".format(oeis_id, bfile_values[:10]))

        values = main_values  # Probably the safest choice.

    if offset_a is not None:

        if offset_a != bfile_first_index:
            logger.error("[A{:06}] (P06) %O directive claims first index is {}, but b-file starts at index {}.".format(oeis_id, offset_a, bfile_first_index))

    if offset_b is not None:

        indexes_where_magnitude_exceeds_1 = [i for i in range(len(values)) if abs(values[i]) > 1]

        if len(indexes_where_magnitude_exceeds_1) > 0:

            first_index_where_magnitude_exceeds_1 = 1 + min(indexes_where_magnitude_exceeds_1)

            if offset_b != first_index_where_magnitude_exceeds_1:
                logger.error("[A{:06}] (P09) %O directive claims first index where magnitude exceeds 1 is {}, but values suggest this should be {}.".format(oeis_id, offset_b, first_index_where_magnitude_exceeds_1))

    # Return parsed values.

    return OeisEntry(oeis_id, identification, values, name, comments, detailed_references, links, formulas, examples,
                     maple_programs, mathematica_programs, other_programs, cross_references, keywords, offset_a, offset_b, author, extensions_and_errors)


def create_database_schema(dbconn):
    """Ensure that the 'oeis_entries' table is present in the database."""

    schema = """
             CREATE TABLE IF NOT EXISTS oeis_entries (
                 oeis_id               INTEGER  PRIMARY KEY NOT NULL, -- OEIS ID number.
                 identification        TEXT,
                 value_list            TEXT,
                 name                  TEXT     NOT NULL,
                 comments              TEXT,
                 detailed_references   TEXT,
                 links                 TEXT,
                 formulas              TEXT,
                 examples              TEXT,
                 maple_programs        TEXT,
                 mathematica_programs  TEXT,
                 other_programs        TEXT,
                 cross_references      TEXT,
                 keywords              TEXT     NOT NULL,
                 offset_a              INTEGER,
                 offset_b              INTEGER,
                 author                TEXT,
                 extensions_and_errors TEXT
             );
             """

    # Remove superfluous whitespace in SQL statement.
    schema = "\n".join(line[13:] for line in schema.split("\n"))[1:-1]

    # Execute the schema creation statement.

    dbconn.execute(schema)


def process_oeis_entry(oeis_entry):

    (oeis_id, main_content, bfile_content) = oeis_entry

    parsed_entry = parse_oeis_content(oeis_id, main_content, bfile_content)

    result = (
        parsed_entry.oeis_id,
        parsed_entry.identification,
        ",".join(str(value) for value in parsed_entry.values),
        parsed_entry.name,
        parsed_entry.comments,
        parsed_entry.detailed_references,
        parsed_entry.links,
        parsed_entry.formulas,
        parsed_entry.examples,
        parsed_entry.maple_programs,
        parsed_entry.mathematica_programs,
        parsed_entry.other_programs,
        parsed_entry.cross_references,
        ",".join(str(keyword) for keyword in parsed_entry.keywords),
        parsed_entry.offset_a,
        parsed_entry.offset_b,
        parsed_entry.author,
        parsed_entry.extensions_and_errors
    )

    return result


def process_database_entries(database_filename_in):

    if not os.path.exists(database_filename_in):
        logger.critical("Database file '{}' not found! Unable to continue.".format(database_filename_in))
        return

    (root, ext) = os.path.splitext(database_filename_in)

    database_filename_out = root + "_parsed" + ext

    if os.path.exists(database_filename_out):
        logger.info("Removing stale file '{}' ...".format(database_filename_out))
        os.remove(database_filename_out)

    # ========== fetch and process database entries, ordered by oeis_id.

    BATCH_SIZE = 1000

    with start_timer() as timer:
        with close_when_done(sqlite3.connect(database_filename_in)) as dbconn_in, close_when_done(dbconn_in.cursor()) as dbcursor_in:
            with close_when_done(sqlite3.connect(database_filename_out)) as dbconn_out, close_when_done(dbconn_out.cursor()) as dbcursor_out:

                create_database_schema(dbconn_out)

                with concurrent.futures.ProcessPoolExecutor() as pool:

                    dbcursor_in.execute("SELECT oeis_id, main_content, bfile_content FROM oeis_entries ORDER BY oeis_id;")

                    while True:

                        oeis_entries = dbcursor_in.fetchmany(BATCH_SIZE)
                        if len(oeis_entries) == 0:
                            break

                        logger.log(logging.PROGRESS, "Parsing OEIS entries A{:06} to A{:06} ...".format(oeis_entries[0][0], oeis_entries[-1][0]))

                        query = "INSERT INTO oeis_entries(oeis_id, identification, value_list, name, comments, detailed_references, links, formulas, examples, maple_programs, mathematica_programs, other_programs, cross_references, keywords, offset_a, offset_b, author, extensions_and_errors) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

                        dbcursor_out.executemany(query, pool.map(process_oeis_entry, oeis_entries))

                        dbconn_out.commit()

        logger.info("Processed all database entries in {}.".format(timer.duration_string()))


def main():

    if len(sys.argv) != 2:
        print("Please specify the name of an OEIS database in Sqlite3 format.")
        return

    database_filename_in = sys.argv[1]

    (root, ext) = os.path.splitext(database_filename_in)
    logfile = root + "_parsed.log"

    with setup_logging(logfile):
        process_database_entries(database_filename_in)


if __name__ == "__main__":
    main()
