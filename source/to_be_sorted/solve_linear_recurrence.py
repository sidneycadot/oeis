#! /usr/bin/env python3

import os
import sys
import logging
import sqlite3
import json
from fractions import Fraction, gcd
from collections import OrderedDict
import concurrent.futures

import numpy as np

from fraction_based_linear_algebra import inverse_matrix
from source.utilities.timer import start_timer
from oeis_entry import parse_oeis_entry
from source.utilities.exit_scope import close_when_done
from source.utilities.setup_logging import setup_logging

logger = logging.getLogger(__name__)


class Term:
    def __init__(self, offset, alpha, beta):
        if offset is None:
            assert alpha is None
        else:
            assert alpha > 0
        self.offset = offset
        self.alpha  = alpha
        self.beta   = beta

    def __call__(self, a, i):
        if self.offset is None:
            return (i ** self.beta)
        else:
            return (a[i - self.offset] ** self.alpha) * (i ** self.beta)

    def __str__(self):

        if self.offset is None:
            if self.beta == 0:
                return "1"
            elif self.beta == 1:
                return "i"
            else:
                return "i^{}".format(self.beta)
        else:
            if self.alpha == 1:
                if self.beta == 0:
                    return "a[i - {}]".format(self.offset)
                elif self.beta == 1:
                    return "a[i - {}] * i".format(self.offset)
                else:
                    return "a[i - {}] * i^{}".format(self.offset, self.beta)
            else:
                if self.beta == 0:
                    return "a[i - {}]^{}".format(self.offset, self.alpha)
                elif self.beta == 1:
                    return "a[i - {}]^{} * i".format(self.offset, self.alpha)
                else:
                    return "a[i - {}]^{} * i^{}".format(self.offset, self.alpha, self.beta)

    def __repr__(self):
        return "Term({}, {}, {})".format(self.offset, self.alpha, self.beta)


def solution_to_string(solution, terms):
    (coefficients, divisor) = solution

    s = ""

    for (c, t) in zip(coefficients, terms):
        st = str(t)
        if c == 0:
            continue
        if abs(c) == 1:
            cs = st
        else:
            if st == "1":
                cs = "{}".format(abs(c))
            else:
                cs = "{} * {}".format(abs(c), st)

        if c < 0:
            if s == "":
                s = "-" + cs
            else:
                s = s + " - " + cs
        else:
            if s == "":
                s = cs
            else:
                s = s + " + " + cs

    if divisor != 1:
        s = "(" + s + ") / {}".format(divisor)

    if s == "":
        s = "0"

    return s


def verify_linear_equation(lookup, terms, coefficients):

    equations_lhs = []
    equations_rhs = []

    for index in lookup:
        try:
            lhs = [Fraction(term_definition(lookup, index)) for term_definition in terms]
            rhs = Fraction(lookup[index])
        except KeyError:
            # Tried to include a value that is not available.
            # Do not add this as an equation.
            pass
        else:
            # We have succeeded in constructing both the LHS and RHS of the equation for this sequence value.
            equations_lhs.append(lhs)
            equations_rhs.append(rhs)

    a = np.array(equations_lhs)
    b = np.array(equations_rhs)

    fit = a.dot(coefficients)

    return np.all(fit == b)


def solve_linear_equation(oeis_id, lookup, terms):

    equations_lhs = []
    equations_rhs = []

    EXTRA_EQUATIONS = 5

    for index in lookup:

        try:
            lhs = [Fraction(term_definition(lookup, index)) for term_definition in terms]
            rhs = Fraction(lookup[index])
        except KeyError:
            # Tried to include a value that is not available.
            # Do not add this as an equation.
            pass
        else:
            # We have succeeded in constructing both the LHS and RHS of the equation for this sequence value.
            equations_lhs.append(lhs)
            equations_rhs.append(rhs)

            if len(equations_lhs) == len(terms) + EXTRA_EQUATIONS:
                # We found a sufficient number of equations, terminate the search.
                break
    else:
        # Candidates exhausted, but not enough equations.
        return None # no solution.

    #logger.info("[A{:06d}] Found {} equations.".format(oeis_id, len(equations_lhs)))

    a = np.array(equations_lhs)
    b = np.array(equations_rhs)

    # Do a least-squares fit.

    #logger.info("[A{:06d}] Do least squares fit...".format(oeis_id))

    try:
        coefficients = inverse_matrix(a.T.dot(a)).dot(a.T).dot(b)
    except ZeroDivisionError:
        # Matrix does not have a unique solution.
        return None

    # First, check if the fit is perfect for our equations.
    # If not, it cannot be correct.
    fit = a.dot(coefficients)
    if np.any(fit != b):
        #logger.info("[A{:06d}] Not a candidate.".format(oeis_id))
        return None

    logger.info("[A{:06d}] Candidate solution found, checking ...".format(oeis_id))

    # Candidate seems legit -- but it may still be a false positive that only works for the selected equations.
    # We now test against *all* equations to make sure.
    if not verify_linear_equation(lookup, terms, coefficients):
        logger.info("[A{:06d}] Candidate verification failed.".format(oeis_id))
        return None

    # determine the least-common-multiple of the coefficient denominators.

    lcm = 1
    for c in coefficients:
        lcm *= (c.denominator // gcd(lcm, c.denominator))

    integer_coefficients = [int(c * lcm) for c in coefficients]

    solution = (integer_coefficients, lcm)

    logger.info("[A{:06d}] Candidate verification successful; solution: a[i] == {}.".format(oeis_id, solution_to_string(solution, terms)))

    return solution


def find_sequence_solution(work):

    (oeis_entry, terms) = work

    if len(oeis_entry.offset) < 1:
        logger.info("[A{:06d}] Skipping sequence without declared first index.".format(oeis_entry.oeis_id))
        solution = None
    else:
        max_value =  max(abs(v) for v in oeis_entry.values)
        max_value_digit_count = len(str(max_value))

        if max_value_digit_count >= 10000:
            logger.info("[A{:06d}] Skipping sequence with very large values ({} digits).".format(oeis_entry.oeis_id, max_value_digit_count))
            solution = None
        else:
            first_index = oeis_entry.offset[0]
            # Turn the sequence data in a lookup dictionary.
            lookup = OrderedDict((i + first_index, v) for (i, v) in enumerate(oeis_entry.values))
            solution = solve_linear_equation(oeis_entry.oeis_id, lookup, terms)

    return (oeis_entry, solution) # None or a 1-dimensional ndarray of coefficients


def make_terms(max_beta_poly, offset_alpha_beta):
    terms = []
    if max_beta_poly is not None:
        for beta_poly in range(0, max_beta_poly + 1):
            term = Term(None, None, beta_poly)
            terms.append(term)
    for (offset, (max_alpha, max_beta)) in enumerate(offset_alpha_beta, 1):
        for alpha in range(1, max_alpha + 1):
            for beta in range(0, max_beta + 1):
                term = Term(offset, alpha, beta)
                terms.append(term)
    return terms


def process_oeis_entry(work):

    (oeis_id, main_content, bfile_content, terms) = work

    parsed_entry = parse_oeis_entry(oeis_id, main_content, bfile_content)

    if parsed_entry.offset_a is None:
        logger.warning("A{:06d} Skipping sequence without declared first index.".format(parsed_entry.oeis_id))
        solution = None
    else:
        max_value = max(abs(v) for v in parsed_entry.values)
        max_value_digit_count = len(str(max_value))

        if max_value_digit_count >= 10000:
            logger.info("[A{:06d}] Skipping sequence with very large values ({} digits).".format(parsed_entry.oeis_id, max_value_digit_count))
            solution = None
        else:
            first_index = parsed_entry.offset_a
            # Turn the sequence data in a lookup dictionary.
            lookup = OrderedDict((first_index + i, value) for (i, value) in enumerate(parsed_entry.values))
            solution = solve_linear_equation(parsed_entry.oeis_id, lookup, terms)

    return (parsed_entry, solution)


def poly_terms_generator():
    n = 0
    filename_out = "solutions_poly_{}.txt".format(n)
    terms = [Term(None, None, i) for i in range(n - 1)]
    yield (filename_out, terms)


def solve_linear_recurrences(database_filename_in: str, terms, exclude_entries = None):

    if not os.path.exists(database_filename_in):
        logger.critical("Database file '{}' not found! Unable to continue.".format(database_filename_in))
        return

    if exclude_entries is None:
        exclude_entries = frozenset()

    # ========== fetch and process database entries, ordered by oeis_id.

    BATCH_SIZE = 1000

    with start_timer() as timer:

        with close_when_done(sqlite3.connect(database_filename_in)) as dbconn_in, close_when_done(dbconn_in.cursor()) as dbcursor_in:

            with concurrent.futures.ProcessPoolExecutor() as pool:

                dbcursor_in.execute("SELECT oeis_id, main_content, bfile_content FROM oeis_entries ORDER BY oeis_id;")

                while True:

                    oeis_entries = dbcursor_in.fetchmany(BATCH_SIZE)
                    if len(oeis_entries) == 0:
                        break

                    logger.log(logging.PROGRESS, "Processing OEIS entries A{:06} to A{:06} ...".format(oeis_entries[0][0], oeis_entries[-1][0]))

                    work = [(oeis_id, main_content, bfile_content, terms) for (oeis_id, main_content, bfile_content) in oeis_entries if "A{:06d}".format(oeis_id) not in exclude_entries]

                    for (oeis_entry, solution) in pool.map(process_oeis_entry, work):
                        if solution is not None:
                            yield (str(oeis_entry), solution)

        logger.info("Processed all database entries in {}.".format(timer.duration_string()))


def solve_polynomials(database_filename_in):

    exclude_entries = set()

    max_degree = 0
    while True:

        # Can we read the solutions from a file?
        solutions_filename = "polynomial_solutions_{}.txt".format(max_degree)

        if os.path.exists(solutions_filename):
            with open(solutions_filename, "r") as f:
                solutions = json.load(f)
            logger.info("Read {} entries from '{}'.".format(len(solutions), solutions_filename))
        else:
            terms = [Term(None, None, degree) for degree in range(max_degree + 1)]
            solutions = list(solve_linear_recurrences(database_filename_in, terms, exclude_entries))
            with open(solutions_filename, "w") as f:
                json.dump(solutions, f)
            logger.info("Wrote {} entries to '{}'.".format(len(solutions), solutions_filename))

        exclude_entries |= set(oeis_string_id for (oeis_string_id, solution) in solutions)

        max_degree += 1


def main():

    if len(sys.argv) != 2:
        print("Please specify the name of an OEIS database in Sqlite3 format.")
        return

    database_filename_in = sys.argv[1]

    logfile = "solve_linear_recurrence.log"

    with setup_logging(logfile):
        logging.getLogger("oeis_entry").setLevel(logging.CRITICAL)
        solve_polynomials(database_filename_in)


if __name__ == "__main__":
    main()
