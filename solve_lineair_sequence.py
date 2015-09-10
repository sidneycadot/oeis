#! /usr/bin/env python3

import pickle
import numpy as np
from fractions import Fraction
from fraction_based_linear_algebra import inverse_matrix

class SafeSequenceAccessor:
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
            pass
        else:
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

def test():

    a = []
    while len(a) < 30:
        i = len(a)
        ai = 100 + 10 * i
        a.append(ai)

    print(a)

    term_definitions = [
        lambda a, i: a[i - 1],
        lambda a, i: 1
    ]

    solution = solve_lineair_equation(a, term_definitions)

    print("solution:", solution)

def main():

    with open("oeis.pickle", "rb") as f:
        entries = pickle.load(f)

    term_definitions = [
        lambda a, i: 1,
        lambda a, i: i % 5
    ]

    print("testing entries ...")

    for entry in entries:
        if len(entry.values) < 10:
            continue
        solution = solve_lineair_equation(entry.values, term_definitions)
        if solution is not None:
            print("{} offset {} length {} solution {} name {}".format(entry, entry.offset, len(entry.values), [float(x) for x in solution], entry.name))
            print("    {}".format(entry.values[:10]))
if __name__ == "__main__":
    main()

