#! /usr/bin/env python3

import pickle
import numpy as np
from fractions import Fraction
from fraction_based_linear_algebra import inverse_matrix
from catalog import catalog

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

def solve_lineair_equation(sequence, term_definitions):

    sequence = SafeSequenceAccessor(sequence)

    equations_lhs = []
    equations_rhs = []

    for i in range(len(sequence)):
        equation = []
        try:
            for term_definition in term_definitions:
                value = Fraction(term_definition(sequence, i))
                equation.append(value)
        except:
            # we tried to read beyond the boundaries of the sequence.
            # Do not add this as an equation.
            pass
        else:
            # we successfully added all values.
            equations_lhs.append(equation)
            equations_rhs.append(Fraction(sequence[i]))

    if len(equations_lhs) == 0:
        return None

    a = np.array(equations_lhs)
    b = np.array(equations_rhs)

    try:
        solution = inverse_matrix(a.T.dot(a)).dot(a.T).dot(b)
    except ValueError:
        return None

    fit = a.dot(solution)
    if np.any(fit != b):
        return None

    return solution

def main():

    filename = "oeis.pickle"
    #filename = "oeis-10000.pickle"

    with open(filename, "rb") as f:
        oeis_entries = pickle.load(f)

    print("size of OEIS database ...... : {:6d}".format(len(oeis_entries)))
    print("size of our catalog ........ : {:6d}".format(len(catalog)))
    print()

    term_definitions = [
        ("1"      , lambda a, i: 1       ),
        ("i"      , lambda a, i: i       ),
        #("i^2"    , lambda a, i: i**2       ),
        #("i^3"    , lambda a, i: i**3       ),
      # ("a[i-1]" , lambda a, i: a[i - 1])
      # ("i*a[i-1]" , lambda a, i: Fraction(a[i-1], i + 1))
    ]

    print("testing OEIS entries ...")

    for oeis_entry in oeis_entries:
        if oeis_entry.oeis_id % 10000 == 0:
            print("testing OEIS entry {} ...".format(oeis_entry.oeis_id))
        if oeis_entry.oeis_id in catalog:
            continue # skip known sequences
        if len(oeis_entry.values) < 10:
            continue
        solution = solve_lineair_equation(oeis_entry.values, [tf for (ts, tf) in term_definitions])
        if solution is not None:
            print("{} offset = {}, length = {}, name = {}".format(oeis_entry, oeis_entry.offset, len(oeis_entry.values), oeis_entry.name))
            print("        values = {}".format(oeis_entry.values[:15]))
            print("        relation --> a[i] == {}".format(" + ".join("{} * {}".format(coefficient, ts) for ((ts, tf), coefficient) in zip(term_definitions, solution) if coefficient != 0)))

if __name__ == "__main__":
    main()
