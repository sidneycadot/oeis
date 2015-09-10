#! /usr/bin/env python3

import numpy as np
from fractions import Fraction
from fractional_linear_algebra import inverse_matrix

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

    a = np.array(equations_lhs)
    b = np.array(equations_rhs)

    solution = inverse_matrix(a.T.dot(a)).dot(a.T).dot(b)

    fit = a.dot(solution)
    if np.any(fit != b):
        return None

    return solution


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

