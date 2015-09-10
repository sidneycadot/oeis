#! /usr/bin/env python3

import numpy as np
from fractions import Fraction

def identity_matrix(n):
    return np.array([[Fraction(i == j) for i in range(n)] for j in range(n)])


def inverse_matrix(X):

    if not ((len(X.shape) == 2) and (X.shape[0] == X.shape[1])):
        raise RuntimeError("matrix is not square (shape = {}".format(X.shape))

    n = X.shape[0]

    X = X.copy() # make a copy, so we can change this copy at will.
    Y = identity_matrix(n)

    # Downward elimination: make lower triangle zero and main diagonal 1.
    for i in range(n):
        # Make sure that we have a nonzero pivot in position (i, i).
        for j in range(i, n):
            if X[j, i] != 0:
                # exchange rows (i, j) if necessary
                if i != j:
                    rows_ij = slice(i, j + 1, j - i)
                    X[rows_ij] = np.flipud(X[rows_ij])
                    Y[rows_ij] = np.flipud(Y[rows_ij])
                # we now have a valid pivot!
                break
        else:
            # no pivot found from (i, i) downwards.
            raise ValueError("matrix is not invertible.")

        # we now have a valid (nonzero) pivot in position (i, i).
        # normalize row i such that the pivot becomes 1.

        Y[i, :] /= X[i, i]
        X[i, :] /= X[i, i]

        # use row i to get all entries below the pivot to zero.
        for j in range(i + 1, n):
            Y[j, :] -= X[j, i] * Y[i, :]
            X[j, :] -= X[j, i] * X[i, :]

    # upward elimination: zero the upper triangle.
    for j in range(n - 2, -1, -1):
        for i in range(j + 1, n):
            Y[j, :] -=  X[j, i] * Y[i, :]
            X[j, :] -=  X[j, i] * X[i, :]

    return Y

def stresstest(SIZE, REPEATS):

    import time
    import random

    singular_count = 0

    t1 = time.time()
    for rep in range(REPEATS):

        a = [Fraction(random.randint(-3, +3)) for i in range(SIZE * SIZE)]
        a = np.array(a).reshape(SIZE, SIZE)

        try:
            ai = inverse_matrix(a)
            assert np.all(a.dot(ai) == identity_matrix(SIZE))
            assert np.all(ai.dot(a) == identity_matrix(SIZE))
        except ValueError:
            singular_count += 1

    t2 = time.time()
    duration = (t2 - t1)

    print("*** SIZE = {} ***".format(SIZE))
    print("    {} inversions in {:.3f} seconds ({:.3f} inversions per second).".format(REPEATS, duration, REPEATS / duration))
    print("    {} inversions failed due to a singular matrix.".format(singular_count))
    print()

if __name__ == "__main__":

    #for SIZE in range(21):

        #a = [Fraction(random.gauss(2, 10)).limit_denominator(100) for i in range(SIZE * SIZE)]
        #a = np.array(a).reshape(SIZE, SIZE)

        #t1 = time.time()
        #ai = inverse_matrix(a)
        #t2 = time.time()

        #duration = (t2 - t1)

        #assert np.all(a.dot(ai) == identity_matrix(SIZE))
        #assert np.all(ai.dot(a) == identity_matrix(SIZE))

        #print("size {} --> ok; duration = {}".format(SIZE, duration))

    stresstest(3, 500)
    stresstest(4, 500)
    stresstest(5, 500)
    stresstest(6, 500)
    stresstest(10, 50)
    stresstest(15, 50)
    stresstest(20, 50)
