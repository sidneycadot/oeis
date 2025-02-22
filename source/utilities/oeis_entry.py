"""A module to parse a fetched OEIS entry and its associated b-file into an OeisEntry instance."""

import re
import collections
from enum import Enum
from typing import NamedTuple, List, Tuple, Optional, Callable

from occurring_characters import occurring_characters_per_directive as acceptable_characters


class OeisEntry(NamedTuple):
    """A class containing all information in an OEIS entry, split out by field."""
    oeis_id: int
    identification: str
    values: List[int]
    name: str
    comments: str
    detailed_references: str
    links: str
    formulas: str
    examples: str
    maple_programs: str
    mathematica_programs: str
    other_programs: str
    cross_references: str
    keywords: List[str]
    offset_a: int
    offset_b: Optional[int]
    author: str
    extensions_and_errors: str

    def __str__(self):
        return "A{:06d}".format(self.oeis_id)


class OeisIssueType(Enum):
    """Kinds of issues that can be detected while parsing an OEIS entry."""

    P01 = "Missing %A directive."
    P02 = "Missing %O directive."
    P03 = "No values listed (empty %S directive)."
    P04 = "The %O directive only has a single value."
    P05 = "Value mismatch between main file and b-file."
    P06 = "The first index claimed by the %O directive doesn't correspond to the first index in the b-file."
    P07 = "The main file has more values than b-file."
    P08 = "The b-file has indexes that are non-sequential."
    P09 = "The first index where the %O directive claims that magnitude exceeds 1 is not consistent with the values."
    P10 = "Unacceptable characters in value of directive."
    P11 = "Keyword occurs multiple times in %K directive value."
    P12 = "The b-file line cannot be parsed."
    P13 = "Unexpected empty keyword in %K directive value."
    P14 = "Unusual %I directive value."
    P15 = "Unexpected keyword in %K directive value."
    P16 = "The directive should have a space before the start of its value."
    P17 = "Main content reconstruction failed."
    P18 = "The directive has a trailing space but no value."
    P19 = "Negative values are present, but 'sign' keyword is missing."
    P20 = "Sequence contains extremely large values."
    P21 = "Keywords 'tabl' and 'tabf' occur together, which should not happen."
    P22 = "Keywords 'nice' and 'less' occur together, which should not happen."
    P23 = "Keywords 'easy' and 'hard' occur together, which should not happen."
    P24 = "Keywords 'nonn' and 'sign' occur together, which should not happen."
    P25 = "Keywords 'full' and 'more' occur together, which should not happen."
    P26 = "Keyword 'allocated' occurs in combination with other keywords, which should not happen."
    P27 = "Keyword 'allocating' occurs in combination with other keywords, which should not happen."
    P28 = "Keyword 'dead' occurs in combination with other keywords, which should not happen."
    P29 = "Keyword 'recycled' occurs in combination with other keywords, which should not happen."
    P30 = "Keyword 'nonn' or 'sign' are both absent."
    P31 = "The entry shouldn't have a b-file that is not system-generated."

class OeisIssue(NamedTuple):
    """Represents an issue found while parsing."""
    oeis_id: int
    issue_type: OeisIssueType
    description: str

# Note that the %V / %W / %X directives have been retired.
expected_directive_order_pattern = re.compile("I(?:S|ST|STU)NC*D*H*F*e*p*t*o*Y*KO?A?E*")

identification_pattern = re.compile("|".join([
    "(?:[MN][0-9]{4}(?: [MN][0-9]{4})* #.*)",
    "(?:[MN][0-9]{4}(?: [MN][0-9]{4})*)",
    "(?:#.*)"
]))

# The expected keywords are documented in three places:
#
# http://oeis.org/eishelp1.html
# http://oeis.org/eishelp2.html                                (more elaborate than the first; documents the keywords "changed", "hear", and "look").
# http://oeis.org/wiki/User:Charles_R_Greathouse_IV/Keywords   (more up-to-date).

expected_keywords = [
    "allocated",   #
    "allocating",  #
    "base",        # dependent on base used for sequence
    "bref",        # sequence is too short to do any analysis with
    "changed",     #
    "cofr",        # a continued fraction expansion of a number
    "cons",        # a decimal expansion of a number
    "core",        # an important sequence
    "dead",        # an erroneous sequence
    "dumb",        # an unimportant sequence
    "dupe",        # duplicate of another sequence
    "easy",        # it is very easy to produce terms of sequence
    "eigen",       # an eigensequence: a fixed sequence for some transformation
    "fini",        # a finite sequence
    "frac",        # numerators or denominators of sequence of rationals
    "full",        # the full sequence is given
    "hard",        # next term not known, may be hard to find. Would someone please extend this sequence?
    "hear",        #
    "less",        # reluctantly accepted
    "look",        #
    "more",        # more terms are needed! would someone please extend this sequence?
    "mult",        # multiplicative: a(mn)=a(m)a(n) if g.c.d.(m,n)=1
    "new",         # new (added within last two weeks, roughly)
    "nice",        # an exceptionally nice sequence
    "nonn",        # a sequence of non-negative numbers
    "obsc",        # obscure, better description needed
    "probation",   #
    "recycled",    #
    "sign",        # sequence contains negative numbers
    "tabf",        # An irregular (or funny-shaped) array of numbers made into a sequence by reading it row by row
    "tabl",        # typically a triangle of numbers, such as Pascal's triangle, made into a sequence by reading it row by row
    "uned",        # not edited
    "unkn",        # little is known; an unsolved problem; anyone who can find a formula or recurrence is urged to let me know.
    "walk",        # counts walks (or self-avoiding paths)
    "word"         # depends on words for the sequence in some language
]

expected_keywords_set = frozenset(expected_keywords)

# A valid b-file line is a (possible negative) index integer, followed by 1 or more tab characters, followed by
# a (possible negative) value integer.
bfile_line_pattern = re.compile("(-?[0-9]+)[ \t]+(-?[0-9]+)")


def count_digits(n: int) -> int:
    """Count the number of decimal digits in an integer."""
    return len(str(abs(n)))


def parse_optional_multiline_directive(dv, directive):
    if directive not in dv:
        return None
    else:
        return "".join(line + "\n" for line in dv[directive])


def parse_mandatory_single_line_directive(dv, directive):
    ok = (directive in dv) and (len(dv[directive]) == 1)
    if not ok:
        raise RuntimeError("Bad mandatory directive.")
    return dv[directive][0]


def parse_optional_single_line_directive(dv, directive):
    if directive not in dv:
        return None
    else:
        ok = (len(dv[directive]) == 1)
        if not ok:
            raise RuntimeError("Bad optional directive.")
        return dv[directive][0]


def parse_value_directives(dv, directive) -> List[int]:

    ok = (directive in dv) and (len(dv[directive]) >= 1)
    if not ok:
        raise RuntimeError("Bad value directive.")

    expect_next = True  # -1 = no, 0 = maybe (first line), 1 = yes

    lines = []

    for line in dv[directive]:
        assert expect_next
        lines.append(line)
        expect_next = line.endswith(",")

    assert not expect_next

    lines = "".join(lines)

    if lines == "":
        return []

    values = [int(value_string) for value_string in lines.split(",")]

    assert ",".join(str(value) for value in values) == lines
    return values


def check_keywords(oeis_id: int, keywords, found_issue: Callable[[OeisIssue], None]) -> None:
    """Check the set of keywords, looking for issues."""

    # Check for unexpected keywords.

    unexpected_keywords = set(keywords) - expected_keywords_set

    for unexpected_keyword in sorted(unexpected_keywords):
        if unexpected_keyword == "":
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P13,
                "Unexpected empty keyword in %K directive value."
            ))
        else:
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P15,
                "Unexpected keyword '{}' in %K directive value.".format(unexpected_keyword)
            ))

    # Check for duplicate keywords.

    keyword_counter = collections.Counter(keywords)
    for (keyword, count) in keyword_counter.items():
        if count > 1:
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P11,
                "Keyword '{}' occurs {} times in %K directive value.".format(keyword, count)
            ))

    # Check forbidden combinations of keywords.

    if "tabl" in keywords and "tabf" in keywords:
        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P21,
            "Keywords 'tabl' and 'tabf' occur together, which should not happen."
        ))

    if "nice" in keywords and "less" in keywords:
        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P22,
            "Keywords 'nice' and 'less' occur together, which should not happen."
        ))

    if "easy" in keywords and "hard" in keywords:
        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P23,
            "Keywords 'easy' and 'hard' occur together, which should not happen."
        ))

    if "nonn" in keywords and "sign" in keywords:
        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P24,
            "Keywords 'nonn' and 'sign' occur together, which should not happen."
        ))

    if "full" in keywords and "more" in keywords:
        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P25,
            "Keywords 'full' and 'more' occur together, which should not happen."
        ))

    # Check exclusive keywords.

    if "allocated" in keywords and len(keywords) > 1:
        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P26,
            "Keyword 'allocated' occurs in combination with other keywords, which should not happen."
        ))

    if "allocating" in keywords and len(keywords) > 1:
        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P27,
            "Keyword 'allocating' occurs in combination with other keywords, which should not happen."
        ))

    if "dead" in keywords and len(keywords) > 1:
        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P28,
            "Keyword 'dead' occurs in combination with other keywords, which should not happen."
        ))

    if "recycled" in keywords and len(keywords) > 1:
        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P29,
            "Keyword 'dead' occurs in combination with other keywords, which should not happen."
        ))

    # Check presence of either 'none' or 'sign' keyword.

    if not(("allocated" in keywords) or ("allocating" in keywords) or ("dead" in keywords) or ("recycled" in keywords)):
        if ("nonn" not in keywords) and ("sign" not in keywords):
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P30,
                "Keyword 'nonn' or 'sign' are both absent."
            ))


def parse_bfile_content(oeis_id: int, bfile_content: str, found_issue: Callable[[OeisIssue], None]) -> Tuple[Optional[int], List[int]]:
    """Parse the content of a b-file.

    A b-file should contain lines with two integer values: an index, and a value.

    The indices should be consecutive.
    """

    lines = bfile_content.splitlines()

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
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P12,
                "The b-file line {} cannot be parsed: '{}'.".format(line_nr, line)
            ))
            break

        index = int(match.group(1))
        value = int(match.group(2))

        if len(indexes) > 0 and (index != indexes[-1] + 1):
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P08,
                "The b-file line {} has indexes that are non-sequential; {} follows {}; terminating parse.".format(
                    line_nr, index, indexes[-1])
            ))
            break

        indexes.append(index)
        values.append(value)

    assert len(indexes) == len(values)

    first_index = indexes[0] if len(indexes) > 0 else None

    return (first_index, values)


def parse_main_content_directives(oeis_id: int, main_content: str) -> List[str]:
    """Split a main-content string into its constituent directives.
    
    Two complications:
    
    (1) some directive content may include newlines.
    (2) Sometimes we see stuff that looks like a directive, but isn't.
    
    Note that the directives V/W/X are no loner used.
    """
    directive_pattern = "^%[ISTUNCDHFeptoYKOAE] A{:06d}".format(oeis_id)
    directive_regexp = re.compile(directive_pattern, re.MULTILINE)

    directive_indices = [m.start() for m in directive_regexp.finditer(main_content)]
    if directive_indices[0] != 0:
        raise ValueError("A{:06d}: the main file doesn't start with the expected directive pattern.".format(oeis_id))

    directive_indices = directive_indices[1:]
    directive_indices.append(len(main_content))

    directives = []
    start_index = 0
    for end_index in directive_indices:
        if main_content[end_index - 1] != '\n':
            raise vValueError("A{:06d}: a directive doesn't end with a newline character".format(oeis_id))
        directives.append((main_content[start_index+1:start_index+2], main_content[start_index+10:end_index - 1]))
        start_index = end_index

    reconstruction = "".join("%{} A{:06d}{}\n".format(directive, oeis_id, content) for (directive, content) in directives)

    if reconstruction != main_content:
        raise ValueError("A{:06d}: the main content cannot be reconstructed from its directives.".format(oeis_id))

    return directives


acceptable_characters = {
}

def parse_main_content(oeis_id, main_content, found_issue: Callable[[OeisIssue], None]) -> Tuple:

    # The order and count of expected directives, for any given entry, is as follows:
    #
    # * %I   one                Identification line.
    # * %STU S/ST/STU           Unsigned values.
    # * %N   one                Name of the sequence.
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

    directives = parse_main_content_directives(oeis_id, main_content)

    # Check the order of the directives found.

    directive_order = "".join(directive for (directive, directive_value) in directives)

    if not expected_directive_order_pattern.fullmatch(directive_order):
        raise RuntimeError("Unexpected directive order: {!r}".format(directive_order))

    # Collect directives

    dv = {}

    for (directive, value) in directives:

        if directive in "STU":
            directive = "STU"

        if len(value) != 0:
            assert value.startswith(" ")
            value = value[1:]

        if directive not in dv:
            dv[directive] = []

        dv[directive].append(value)

        #unacceptable_characters = set(value) - acceptable_characters[directive]
        #if unacceptable_characters:
        #    found_issue(OeisIssue(
        #        oeis_id,
        #        OeisIssueType.P10,
        #        "Unacceptable characters in value of %{} directive ({!r}): {}.".format(
        #            directive, value, ", ".join(["{!r}".format(c) for c in sorted(unacceptable_characters)])
        #        )
        #    ))

    # Parse all directives.

    identification        = parse_mandatory_single_line_directive (dv, 'I')
    stu_values            = parse_value_directives                (dv, "STU")
    name                  = parse_mandatory_single_line_directive (dv, 'N')
    comments              = parse_optional_multiline_directive    (dv, 'C')
    detailed_references   = parse_optional_multiline_directive    (dv, 'D')
    links                 = parse_optional_multiline_directive    (dv, 'H')
    formulas              = parse_optional_multiline_directive    (dv, 'F')
    examples              = parse_optional_multiline_directive    (dv, 'e')
    maple_programs        = parse_optional_multiline_directive    (dv, 'p')
    mathematica_programs  = parse_optional_multiline_directive    (dv, 't')
    other_programs        = parse_optional_multiline_directive    (dv, 'o')
    cross_references      = parse_optional_multiline_directive    (dv, 'Y')
    keywords              = parse_mandatory_single_line_directive (dv, 'K')
    offset                = parse_optional_single_line_directive  (dv, 'O')
    author                = parse_optional_single_line_directive  (dv, 'A')
    extensions_and_errors = parse_optional_multiline_directive    (dv, 'E')

    # Process the %K directive.
    # We parse the keywords first, since they may influence the warnings.

    keywords = keywords.split(",")

    check_keywords(oeis_id, keywords, found_issue)

    # Canonify keywords: remove empty keywords and duplicates. We do not sort, though.

    canonized_keywords = []
    for keyword in keywords:
        if not (keyword == "" or keyword in canonized_keywords):
            canonized_keywords.append(keyword)

    # Process %I directive.

    if identification == "":
        identification = None
    else:
        if identification_pattern.fullmatch(identification) is None:
            print("@@", oeis_id, repr(identification))
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P14,
                "Unusual %I directive value: '{}'.".format(identification)
            ))

    # Process value directives (%S/%T/%U --> STU).

    if len(stu_values) == 0:
        if "allocated" not in keywords:
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P03,
                "No values listed (empty %S directive)."
            ))

    # We no longer need to merge STU and VWX values; the latter are no longer used.

    main_values = stu_values

    if ("dead" not in keywords) and ("sign" not in keywords) and any(value < 0 for value in main_values):
        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P19,
            "Negative values are present, but 'sign' keyword is missing."
        ))

    if len(main_values) > 0:
        max_digits = max(count_digits(v) for v in main_values)
        if max_digits > 1000:
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P20,
                "Sequence contains extremely large values (up to {} digits).".format(max_digits)
            ))

    # Process %A directive.

    if author is None:
        if ("dead" not in keywords) and ("allocated" not in keywords):
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P01,
                "Missing %A directive."
            ))

    # Process %O directive.

    if offset is None:
        if "allocated" not in keywords:
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P02,
                "Missing %O directive in entry that doesn't have the 'allocated' keyword."
            ))
        offset_a = None
        offset_b = None
    else:
        offset_values = [int(o) for o in offset.split(",")]
        if len(offset_values) == 1:
            offset_a = offset_values[0]
            offset_b = None
        elif len(offset_values) == 2:
            offset_a = offset_values[0]
            offset_b = offset_values[1]
        else:
            raise RuntimeError("Unexpected number of offset values.")

    # Return the data parsed from the main_content.

    return (identification, main_values, name, comments, detailed_references, links, formulas, examples,
            maple_programs, mathematica_programs, other_programs, cross_references, canonized_keywords,
            offset_a, offset_b, author, extensions_and_errors)


def calc_lines_needed(values, max_line_length):
    if len(values) == 0:
        return 0
    lengths = [len(str(value))+1 for value in values]
    lengths[-1] -= 1  # The last value doesn't need a comma after it.

    current_line = 1
    current_pos_in_line = 0
    for length in lengths:
        place_on_current_line = (current_pos_in_line == 0 or current_pos_in_line + length <= max_line_length)
        if not place_on_current_line:
            current_line += 1
            current_pos_in_line = 0
        current_pos_in_line += length
    return current_line


def parse_oeis_entry(oeis_id: int, main_content: str, bfile_content: str, found_issue: Callable[[OeisIssue], None]) -> OeisEntry:

    (identification, main_values, name, comments, detailed_references, links, formulas, examples,
     maple_programs, mathematica_programs, other_programs, cross_references, keywords,
     offset_a, offset_b, author, extensions_and_errors) = \
        parse_main_content(oeis_id, main_content, found_issue)

    (bfile_first_index, bfile_values) = parse_bfile_content(oeis_id, bfile_content, found_issue)

    # Merge values obtained from S/T/U directives in main_content with the b-file values.

    if len(main_values) > len(bfile_values):
        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P07,
            "Main file has more values than b-file (main: {}, b-file: {}).".format(
                len(main_values), len(bfile_values))
        ))

    if all(bfile_values[i] == main_values[i] for i in range(min(len(main_values), len(bfile_values)))):
        # The values are fully consistent.
        # Use the one that has the most entries.
        values = bfile_values if len(bfile_values) > len(main_values) else main_values
    else:
        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P05,
            "Value mismatch between main file and b-file (main: {} ; b-file: {}).".format(
                main_values[:10], bfile_values[:10])
        ))

        # In case of disagreement between the main file and the b-file,
        # the main file values are the safest choice.
        values = main_values

    lines_needed = calc_lines_needed(values, 90)
    if lines_needed <= 3:
        # The values can fit in the %S %T %U directives.
        if not ("b-file synthesized from sequence entry" in bfile_content):
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P31,
                "A b-file is present, but the values can fit in {} lines.".format(lines_needed)
            ))

    if offset_a is not None:

        if offset_a != bfile_first_index:
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P06,
                "%O directive claims first index is {}, but b-file starts at index {}.".format(
                    offset_a, bfile_first_index)
            ))

    # The following OEIS entries have a known offset_b value beyond the available values:
    offset_b_hardcoded = {
        93957: 343,
        93958: 3908,
        216280 : 635318657,
        216284 : 635318657,
        327861 : 4153248
    }

    expected_offset_b_value = offset_b_hardcoded.get(oeis_id)

    if expected_offset_b_value is None:
        
        indices_where_magnitude_exceeds_one = [i for i in range(len(values)) if abs(values[i]) > 1]
        
        if len(indices_where_magnitude_exceeds_one) > 0:
            expected_offset_b_value = 1 + min(indices_where_magnitude_exceeds_one)
            expected_offset_b_value_str = str(expected_offset_b_value)
        else:
            expected_offset_b_value = 1
            expected_offset_b_value_str = "1 (none)"

    if offset_b is None:

        found_issue(OeisIssue(
            oeis_id,
            OeisIssueType.P04,
            "The %O directive has no second value that indicates where the sequence magnitude first exceeds 1; we'd expect that value to be {}.".format(expected_offset_b_value_str)
        ))

    else:

        if offset_b != expected_offset_b_value:
            found_issue(OeisIssue(
                oeis_id,
                OeisIssueType.P09,
                "%O directive second value claims thet the first element where magnitude exceeds 1 is at position {}, but values suggest this should be {}.".format(offset_b, expected_offset_b_value_str)
            ))

    # Return parsed values as an OeisEntry.

    return OeisEntry(oeis_id, identification, values, name, comments, detailed_references, links, formulas, examples,
                     maple_programs, mathematica_programs, other_programs, cross_references, keywords,
                     offset_a, offset_b, author, extensions_and_errors)
