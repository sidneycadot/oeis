#! /usr/bin/env python3

import logging
import pickle
import numpy as np
from fractions import Fraction, gcd
from fraction_based_linear_algebra import inverse_matrix
import itertools
import functools
from collections import OrderedDict
from catalog import read_catalog_files
from timer import start_timer
import concurrent.futures

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
                    return "a[i - {}]^{}".format(self.offset, self.alpha, self.beta)
                elif self.beta == 1:
                    return "a[i - {}]^{} * i".format(self.offset, self.alpha, self.beta)
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

def solve_lineair_equation(oeis_id, lookup, terms):

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
            solution = solve_lineair_equation(oeis_entry.oeis_id, lookup, terms)

    return (oeis_entry, solution) # None or a 1-dinmensional ndarray of coefficients

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

def solve_sequences():

    catalog = read_catalog_files("catalog_files/*.json")

    filename = "oeis_with_bfile-10000.pickle"

    with open(filename, "rb") as f:
        oeis_entries = pickle.load(f)

    terms = make_terms(5, [(3, 3)])

    #solution = find_sequence_solution((oeis_entries[8553 - 1], terms))
    #return

    work = [(oeis_entry, terms) for oeis_entry in oeis_entries if oeis_entry.oeis_id not in catalog and len(oeis_entry.offset) >= 1]

    logger.info("size of work ...... : {:6d}".format(len(work)))
    logger.info("terms ............. : {}".format(", ".join(repr(t) for t in terms)))

    with open("solutions.json", "w") as f, concurrent.futures.ProcessPoolExecutor() as executor:

        for (oeis_entry, solution) in executor.map(find_sequence_solution, work):
            if solution is not None:
                #print("[{}] ({} elements) solution: {}".format(oeis_entry, len(oeis_entry.values), solution))
                (coefficients, divisor) = solution
                while len(coefficients) > 0 and coefficients[-1] == 0:
                    coefficients = coefficients[:-1]

                #print(json_entry)
                #print(json_entry, file = f)
            if oeis_entry.oeis_id % 10 == 0:
                logger.log(logging.PROGRESS, "[{}] processed.".format(oeis_entry))

def main():

    logging.PROGRESS = logging.DEBUG + 5
    logging.addLevelName(logging.PROGRESS, "PROGRESS")

    FORMAT = "%(asctime)-15s | %(levelname)-8s | %(message)s"
    logging.basicConfig(format = FORMAT, level = logging.DEBUG)

    try:
        with start_timer() as timer:
            solve_sequences()
            logger.info("Sequence solving completed in {}.".format(timer.duration_string()))
    finally:
        logging.shutdown()

if __name__ == "__main__":
    main()
