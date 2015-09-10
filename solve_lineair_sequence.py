#! /usr/bin/env python3

import numpy as np
from fractions import Fraction

a = [0, 1]
while len(a) < 20:
    i = len(a)
    ai = 2 * a[i -2] ** 2 + a[i - 1] + i ** 2
    a.append(ai)

PREVIOUS_TERM_POWERS = [(-2, 2), (-2, 1), (-1, 2), (-1, 1)]
INDEX_POWERS = [0, 1, 2]

equations_lhs = []
equations_rhs = []

for i in range(len(a)):
    equation = []
    for (shift, exponent) in PREVIOUS_TERM_POWERS:
        ishift = i + shift
        if not (0 <= ishift < len(a)):
            break
        e = a[ishift] ** exponent
        try:
            e = float(e)
        except OverflowError:
            break
        equation.append(e)

    if len(equation) != len(PREVIOUS_TERM_POWERS):
        print("cannot make equation for i = {}".format(i))
        continue # we cannot synthesize this equation because we depend on values that are not available.

    for exponent in INDEX_POWERS:
        equation.append(float(i ** exponent))

    print("*** adding equation for a({}): {} == {}".format(i, equation, a[i]))

    equations_lhs.append(equation)
    equations_rhs.append(a[i])

a = np.array(equations_lhs, dtype = np.float64)
b = np.array(equations_rhs, dtype = np.float64)

print(a.shape, a.dtype)
print(b.shape, b.dtype)

(x, residuals, rank, s) = np.linalg.lstsq(a, b)

print("x", x)
x = [Fraction(c).limit_denominator(10) for c in x]
print("x", x)

print("residuals", residuals)
print("rank", rank)
print("s", s)
