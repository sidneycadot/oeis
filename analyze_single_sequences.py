#! /usr/bin/env python3

import pickle
import numpy as np
import warnings

warnings.simplefilter('ignore')

EPSILON = 1e-6

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
        # unable to interpret offset
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

        yfit = np.polyval(fitpoly, x)

        if np.all(np.abs(yfit - y) < EPSILON):
            print("[{}] found polynomial relation: {} name: {}".format(entry, fitpoly, entry.name))
