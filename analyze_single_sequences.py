#! /usr/bin/env python3

import pickle
import numpy as np
import warnings

warnings.simplefilter('ignore')

EPSILON = 1e-10

def gcd(a, b):
    a = abs(a)
    b = abs(b)
    while a > 0:
        (a, b) = (b % a, a)
    return b

def gcd_many(seq):
    g = 0
    for s in seq:
        g = gcd(g, s)
    return g

with open("oeis.pickle", "rb") as f:
    entries = pickle.load(f)

for entry in entries:

    if entry.oeis_id % 1000 == 0:
        print("[{}] scanning ...".format(entry))

    if len(entry.offset) != 2:
        # unable to interpret offset
        continue

    n = len(entry.values)

    if len(entry.values) < 5:
        # don't try short sequences
        continue

    x = entry.offset[0] + np.arange(n)
    y = entry.values

    #print(entry)
    #print("x", x)
    #print("y", y)
    #print()

    degree = min(n - 1, 10)

    fitpoly = np.polyfit(x, y, degree)
    assert len(fitpoly) == degree + 1

    fitpoly[np.abs(fitpoly) < EPSILON] = 0

    while len(fitpoly) > 0 and fitpoly[0] == 0:
        fitpoly = fitpoly[1:]

    if len(fitpoly) <= n - 5:

        DENOM = 3628800 # == factorial(10)
        fitpoly = np.round(fitpoly * 3628800).astype(np.int)

        G = gcd_many(fitpoly)

        if G == 0:
            DENOM = 1
        else:
            fitpoly //= G
            DENOM   //= G

        yfit = np.polyval(fitpoly, x) / DENOM

        if np.all(np.abs(yfit - y) < EPSILON):
            print("[{}] <len: {} first_index: {}> found polynomial relation: {} / {} -- name: {}".format(entry, len(entry.values), entry.first_index, fitpoly, DENOM, entry.name))
