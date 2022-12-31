#! /usr/bin/env python3

from fractions import Fraction

import numpy as np


def identity_matrix(n):
    """Construct an identity matrix I."""

    return np.array([[Fraction(i == j) for i in range(n)] for j in range(n)])


def inverse_matrix(X):
    """Calculate the inverse of matrix X that consists of Fractions.

    If X is non-square, a ValueError will be raised.
    If X is singular (non-invertible), a ZeroDivisionError will be raised.
    """

    ok = (len(X.shape) == 2) and (X.shape[0] == X.shape[1])

    if not ok:
        raise ValueError("matrix is not square (shape = {})".format(X.shape))

    n = X.shape[0]

    # Construct a matrix XI that is the square matrix X with the identity matrix I
    # directly to its right: XI = [X I].
    #
    # The inversion algorithm performs row operations on XI that will change the
    # left (X) part into the identity matrix:
    #
    # Oₙ · … · O₂ · O₁ · [X I] == [I X⁻¹]
    #
    # The matrix product On · … · O₂ · O₁ is equal to the inverse of matrix X.
    # This implies that these operations have taken the right-hand side of
    # matrix [X I], which started out as an identity matrix, into X⁻¹.

    I = identity_matrix(n)

    XI = np.hstack((X, I))  # Join the X and I matrices.

    del X, I  # Remove them from the scope to prevent accidental usage.

    # Downward elimination: perform row operations that make the lower triangle
    # of X equal to 0 and the main diagonal 1.
    #
    # In this first elimination stage, we may encounter pivots that cannot be
    # made zero by row exchanges. If that happens, the matrix is singular
    # (non-invertible). This is indicated by a ValueError exception.

    for i in range(n):
        # Make sure that we have a nonzero pivot in position (i, i).
        for j in range(i, n):
            if XI[j, i] != 0:
                # Exchange rows (i, j) if necessary.
                if i != j:
                    rows_ij = slice(i, j + 1, j - i)
                    XI[rows_ij] = np.flipud(XI[rows_ij])
                # Position (i, j) now have a valid (nonzero) pivot.
                break
        else:
            # No pivot found from (i, i) downwards. The matrix is singular.
            raise ZeroDivisionError("matrix is singular")

        # There is a valid (nonzero) pivot in position (i, i).
        # Normalize row i such that the pivot position becomes 1.

        XI[i, :] /= XI[i, i]

        # Use row i to get all entries below the pivot to zero.
        for j in range(i + 1, n):
            XI[j, :] -= XI[j, i] * XI[i, :]

    # Upward elimination: zero the upper triangle using row operations.
    for j in range(n - 2, -1, -1):
        for i in range(j + 1, n):
            XI[j, :] -= XI[j, i] * XI[i, :]

    return XI[:, -n:]


def stresstest(SIZE, REPEATS):
    """Perform a randomized stress test on SIZE x SIZE matrices."""

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
        except ZeroDivisionError:
            singular_count += 1

    t2 = time.time()
    duration = (t2 - t1)

    print("*** SIZE = {}:".format(SIZE))
    print()
    print("    {} inversions in {:.3f} seconds ({:.3f} inversions per second).".format(REPEATS, duration, REPEATS / duration))
    print("    {} inversions failed due to a singular matrix.".format(singular_count))
    print()


def main():
    """Perform stress tests on small and medium-sized matrices."""

    stresstest(1, 500)
    stresstest(2, 500)
    stresstest(3, 500)
    stresstest(4, 500)
    stresstest(5, 500)
    stresstest(10, 50)
    stresstest(15, 50)
    stresstest(20, 50)


if __name__ == "__main__":
    main()
