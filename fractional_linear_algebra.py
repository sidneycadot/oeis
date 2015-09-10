#! /usr/bin/env python3

import numpy as np
from fractions import Fraction

def identity_matrix(n):
    return np.array([[Fraction(i == j) for i in range(n)] for j in range(n)])

def inverse_matrix(X):

    assert len(X.shape) > 0
    n = X.shape[0]
    assert X.shape == (n, n)

    X = X.copy()
    Y = identity_matrix(n)

    # downward elimination: make lower triangle zero and main diagonal 1.
    for i in range(n):
        # make sure that we have a nonzero pivot in position (i, i)
        for j in range(i, n):
            if X[j, i] != 0:
                # exchange rows (i, j) if necessary
                if i != j:
                    X[i:j+1:j] = numpy.flipud(X[i:j+1:j])
                    Y[i:j+1:j] = numpy.flipud(Y[i:j+1:j])
                # we now have a valid pivot!
                break
        else:
            # no pivot found from (i, i) downwards.
            raise RuntimeError("matrix is not invertible.")

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

if __name__ == "__main__":

    import random
    import numpy as np
    import time

    for SIZE in range(21):

        a = [Fraction(random.gauss(2, 10)).limit_denominator(100) for i in range(SIZE * SIZE)]
        a = np.array(a).reshape(SIZE, SIZE)

        t1 = time.time()
        ai = inverse_matrix(a)
        t2 = time.time()

        duration = (t2 - t1)

        assert np.all(a.dot(ai) == identity_matrix(SIZE))
        assert np.all(ai.dot(a) == identity_matrix(SIZE))

        print("size {} --> ok; duration = {}".format(SIZE, duration))

