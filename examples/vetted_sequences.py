#! /usr/bin/env python3

from collections import OrderedDict

# =====================================================================

def isprime(n):
    if n < 2:
        return False
    d = 2
    while d * d <= n:
        if n % d == 0:
            return False
        d += 1
    return True

def gcd(a, b):
    while a:
        (a, b) = (b % a, a)
    return b

def count_divisors(n):
    return sum(1 for d in range(1, n + 1) if n % d == 0)

def sum_divisors(n):
    return sum(d for d in range(1, n + 1) if n % d == 0)

def euler_phi(n):
    return sum(1 for d in range(1, n + 1) if gcd(d, n) == 1)

# =====================================================================

class Sequence:
    def __init__(self, first_index, last_index):
        self._first_index = first_index
        self._last_index  = last_index

    @property
    def first_index(self):
        return self._first_index

    @property
    def last_index(self):
        return self._last_index

    def __getitem__(self, index):
        return self._value(index)

    def sequence_string(self, max_chars):
        s = ""
        i = self._first_index
        while True:
            if self._last_index is not None and i == self._last_index:
                break
            next_value = self[i]
            if s == "":
                next_s = str(next_value)
            else:
                next_s = s + ", " + str(next_value)
            if max_chars is not None and len(next_s) > max_chars:
                break
            s = next_s
            i += 1
        return s

class PrimeSequence(Sequence):
    def __init__(self):
        Sequence.__init__(self, 1, None)
        self._memo = [2]

    def __repr__(self):
        return "PrimeSequence()"

    def _value(self, index):
        while len(self._memo) < index:
            # add next prime
            p = self._memo[-1] + 1
            while not isprime(p):
                p += 1
            self._memo.append(p)

        return self._memo[index - 1]

class TwinPrimeSequence(Sequence):
    def __init__(self):
        Sequence.__init__(self, 1, None)
        self._memo = [3]

    def __repr__(self):
        return "TwinPrimeSequence()"

    def _value(self, index):
        while len(self._memo) < index:
            p = self._memo[-1] + 2
            while not (isprime(p) and isprime(p + 2)):
                p += 2
            self._memo.append(p)

        return self._memo[index - 1]

class PrimePiSequence(Sequence):
    def __init__(self):
        Sequence.__init__(self, 1, None)
        self._memo = [0]

    def __repr__(self):
        return "PrimePiSequence()"

    def _value(self, index):
        while len(self._memo) < index:
            nextvalue = self._memo[-1] + int(isprime(index))
            self._memo.append(nextvalue)

        return self._memo[index - 1]

class CountDivisorsSequence(Sequence):
    """ tau
    """
    def __init__(self):
        Sequence.__init__(self, 1, None)

    def __repr__(self):
        return "CountDivisorsSequence()"

    def _value(self, index):
        return count_divisors(index)

class SumDivisorsSequence(Sequence):
    """
    """
    def __init__(self):
        Sequence.__init__(self, 1, None)

    def __repr__(self):
        return "SumDivisorsSequence()"

    def _value(self, index):
        return sum_divisors(index)

class EulerPhiSequence(Sequence):
    """ EulerPhi
    """
    def __init__(self):
        Sequence.__init__(self, 1, None)

    def __repr__(self):
        return "EulerPhiSequence()"

    def _value(self, index):
        return euler_phi(index)

class PolynomialSequence(Sequence):
    def __init__(self, first_index, coefficients):
        Sequence.__init__(self, first_index, None)
        self._first_index  = first_index
        self._coefficients = coefficients

    def __repr__(self):
        return "PolynomialSequence({!r}, {!r})".format(self._first_index, self._coefficients)

    def _value(self, index):

        value = sum(self._coefficients[i] * (index ** i) for i in range(len(self._coefficients)))

        return value

class FibonacciLikeSequence(Sequence):
    def __init__(self, initial_values, coefficients):
        assert len(initial_values) == len(coefficients)
        Sequence.__init__(self, 0, None)
        self._initial_values = initial_values
        self._coefficients = coefficients
        self._memo = {}

    def __repr__(self):
        return "FibonacciLikeSequence({!r}, {!r})".format(self._initial_values, self._coefficients)

    def _value(self, index):

        if index < len(self._initial_values):
            return self._initial_values[index]

        if index in self._memo:
            return self._memo[index]

        value = sum(self._value(index - len (self._coefficients) + i) * self._coefficients[i] for i in range(len(self._coefficients)))

        self._memo[index] = value

        return value

class TreeCountSequence(Sequence):
    def __init__(self, fanout):
        Sequence.__init__(self, 1, None)
        self._fanout = fanout
        self._memo = {}

    def __repr__(self):
        return "TreeCountSequence({!r})".format(self._fanout)

    def _count_trees(self, n, m):
        """ We must assign 'n' leaves.
            At the current level of the tree, 'm' nodes are open.
            Each of the 'm' nodes must either be split up ('fanout'), or assigned.
        """

        if n == m:
            return 1

        if n < m:
            return 0

        # We know now that n > m.

        if m == 0:
            return 0

        key = (n, m)
        if key in self._memo:
            return self._memo[key]

        value = sum(self._count_trees(n -  i, (m - i) * self._fanout) for i in range(m + 1))

        self._memo[key] = value

        return value

    def _value(self, index):
        return self._count_trees(1 + (self._fanout - 1) * (index - 1), 1)

oeis_sequences = OrderedDict([
          (     5 , lambda : CountDivisorsSequence()),
          (    10 , lambda : EulerPhiSequence()),
          (    32 , lambda : FibonacciLikeSequence([2, 1], [1, 1])),
          (    40 , lambda : PrimeSequence()),
          (    45 , lambda : FibonacciLikeSequence([0, 1], [1, 1])),
          (    73 , lambda : FibonacciLikeSequence([0, 1, 1], [1, 1, 1])),
          (   203 , lambda : SumDivisorsSequence()),
          (   720 , lambda : PrimePiSequence()),
          (  1045 , lambda : FibonacciLikeSequence([0, 1], [2, 1])),
          (  1359 , lambda : TwinPrimeSequence()),
          (  1590 , lambda : FibonacciLikeSequence([0, 1, 2], [1, 1, 1])),
          (  2572 , lambda : TreeCountSequence(2)),
          ( 10892 , lambda : FibonacciLikeSequence([1, 1], [-1, 1])),
          ( 48654 , lambda : FibonacciLikeSequence([1, 4], [1, 2])),
          ( 85959 , lambda : PolynomialSequence(0, [0, 37])),
          (104449 , lambda : FibonacciLikeSequence([3, 1], [1, 1])),
          (176485 , lambda : TreeCountSequence(3)),
          (176503 , lambda : TreeCountSequence(4)),
          (194628 , lambda : TreeCountSequence(5)),
          (194629 , lambda : TreeCountSequence(6)),
          (194630 , lambda : TreeCountSequence(7)),
          (194631 , lambda : TreeCountSequence(8)),
          (194632 , lambda : TreeCountSequence(9)),
          (194633 , lambda : TreeCountSequence(10)),
    ])

def main():
    MAX_CHARS  = 200
    for key in sorted(oeis_sequences.keys()):
        sequence_maker = oeis_sequences[key]
        sequence = sequence_maker()

        print("A{:06d} : {}".format(key, sequence.sequence_string(max_chars = MAX_CHARS)))

    print()

    for key in sorted(oeis_sequences.keys()):
        sequence_maker = oeis_sequences[key]
        sequence = sequence_maker()

        print("A{:06d} : {}".format(key, repr(sequence)))

if __name__ == "__main__":
    main()
