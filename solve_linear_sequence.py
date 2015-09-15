#! /usr/bin/env python3

import pickle
import numpy as np
from fractions import Fraction, gcd
from fraction_based_linear_algebra import inverse_matrix
from catalog import catalog
import multiprocessing
import itertools
import functools

class SafeSequenceAccessor:
    """ The Safe Sequence Accessor is a wrapper around a sequence
        that only allows indexing from 0 .. len(sequence) - 1.
        Regular sequences (eg tuples, list) also allow negative
        indexes to count from the end of the sequence.
    """
    def __init__(self, sequence):
        self._sequence = sequence
    def __getitem__(self, index):
        if not (0 <= index < len(self._sequence)):
            raise IndexError
        return self._sequence[index]
    def __len__(self):
        return len(self._sequence)

def verify_linear_equation(sequence, first_index, term_definitions, coefficients):

    first_index = int(first_index)

    sequence = SafeSequenceAccessor(sequence)

    equations_lhs = []
    equations_rhs = []

    for i in range(len(sequence)):
        equation = []
        try:
            for term_definition in term_definitions:
                value = Fraction(term_definition(sequence, first_index + i))
                equation.append(value)
        except:
            # Tried to read beyond the boundaries of the sequence.
            # Do not add this as an equation.
            pass
        else:
            # Successfully added all values.
            equations_lhs.append(equation)
            equations_rhs.append(Fraction(sequence[i]))

    if len(equations_lhs) == 0:
        return False

    a = np.array(equations_lhs)
    b = np.array(equations_rhs)

    fit = a.dot(coefficients)
    if np.any(fit != b):
        return False

    return True

def solve_lineair_equation(sequence, first_index, term_definitions):

    # Turn sequence in a 'SafeSequenceAccessor' that throws an exception
    # whenever someone tries to access it out of its bounds.
    sequence = SafeSequenceAccessor(sequence)

    equations_lhs = []
    equations_rhs = []

    EXTRA_EQUATIONS = 5

    for candidate in range(len(sequence)):

        # 'i' must be a regular int, otherwise the Fraction constructor will fail.
        i = int(first_index + candidate)

        try:
            lhs = [Fraction(term_definition(sequence, i)) for term_definition in term_definitions]
            rhs = Fraction(sequence[i])
        except:
            #Tried to read beyond the boundaries of the sequence.
            # Do not add this as an equation.
            pass
        else:
            # We have succeeded in constructing both the LHS and RHS of the equation for this sequence value.
            equations_lhs.append(lhs)
            equations_rhs.append(rhs)

            if len(equations_lhs) == len(term_definitions) + EXTRA_EQUATIONS:
                # We found a sufficient number of equations, terminate the search.
                break
    else:
        # Candidates exhausted, but not enough equations.
        return None

    a = np.array(equations_lhs)
    b = np.array(equations_rhs)

    # Do a least-squares fit.

    try:
        coefficients = inverse_matrix(a.T.dot(a)).dot(a.T).dot(b)
    except ValueError:
        return None

    # First, check if the fit is perfect for our equations.
    # If not, it cannot be correct.
    fit = a.dot(coefficients)
    if np.any(fit != b):
        return None

    # Candidate seems legit -- but it may still be a false positive that only works for the selected equations.
    # We now test against *all* equations to make sure.
    if not verify_linear_equation(sequence, first_index, term_definitions, coefficients):
        return None

    # determine the least-common-multiple of the coefficient denominators.

    lcm = 1
    for c in coefficients:
        lcm *= (c.denominator // gcd(lcm, c.denominator))

    integer_coefficients = [int(c * lcm) for c in coefficients]

    solution = (integer_coefficients, lcm)

    return solution

def find_solution(work):
    (oeis_entry, term_definitions) = work

    if len(oeis_entry.offset) >=1:
        first_index = oeis_entry.offset[0]
    else:
        first_index = 0

    solution = solve_lineair_equation(oeis_entry.values, first_index, term_definitions)

    return (oeis_entry, solution) # None or a 1-dinmensional ndarray of coefficients

def term_i_power(a, i, exponent):
    return i ** exponent

def term_earlier_a(a, i, offset):
    return a[i - offset]

def main():

    #filename = "oeis.pickle"
    filename = "oeis_with_bfile-10000.pickle"

    with open(filename, "rb") as f:
        oeis_entries = pickle.load(f)

    print("size of OEIS database ...... : {:6d}".format(len(oeis_entries)))
    print("size of our catalog ........ : {:6d}".format(len(catalog)))
    print()

    term_definitions = [functools.partial(term_i_power, exponent = e) for e in range(0, 41)]

    print("testing OEIS entries ...")

    #if oeis_entry.oeis_id in catalog:
    #    continue # skip known sequences

    work = [(oeis_entry, term_definitions) for oeis_entry in oeis_entries]

    pool = multiprocessing.Pool()
    try:

        for (oeis_entry, solution) in pool.imap(find_solution, work):
            if solution is not None:
                print("[{}] ({} elements) solution: {}".format(oeis_entry, len(oeis_entry.values), solution))
            if oeis_entry.oeis_id % 1 == 0:
                print("[{}] ({} elements) -- processed.".format(oeis_entry, len(oeis_entry.values)))

    finally:
        pool.close()
        pool.join()

if __name__ == "__main__":
    main()
