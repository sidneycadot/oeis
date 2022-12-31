
import logging
import glob
import json
from collections import OrderedDict
from source.utilities.timer import start_timer

logger = logging.getLogger(__name__)

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


def smallest_divisor(n):
    if n == 1: # special case
        return 1
    return min(d for d in range(2, n + 1) if n % d == 0)


def count_divisors(n):
    return sum(1 for d in range(1, n + 1) if n % d == 0)


def sum_divisors(n):
    return sum(d for d in range(1, n + 1) if n % d == 0)


def euler_phi(n):
    return sum(1 for d in range(1, n + 1) if gcd(d, n) == 1)

# =====================================================================


class Sequence:
    def __init__(self, first_index, last_index):
        assert isinstance(first_index, int)
        assert last_index is None or isinstance(last_index, int)
        if isinstance(first_index, int) and isinstance(last_index, int):
            assert first_index <= last_index
        self._first_index = first_index
        self._last_index  = last_index

    @property
    def first_index(self):
        return self._first_index

    @property
    def last_index(self):
        return self._last_index

    def __getitem__(self, index):
        if (index < self._first_index) or (self._last_index is not None and index > self._last_index):
            raise IndexError
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

# =====================================================================


class PrimeSequence(Sequence):
    def __init__(self):
        Sequence.__init__(self, 1, None)
        self._memo = [2]

    def __repr__(self):
        return "PrimeSequence()"

    def _value(self, n):
        while len(self._memo) < n:
            # add next prime
            p = self._memo[-1] + 1
            while not isprime(p):
                p += 1
            self._memo.append(p)

        return self._memo[n - 1]


class TwinPrimeSequence(Sequence):
    def __init__(self):
        Sequence.__init__(self, 1, None)
        self._memo = [3]

    def __repr__(self):
        return "TwinPrimeSequence()"

    def _value(self, n):
        while len(self._memo) < n:
            p = self._memo[-1] + 2
            while not (isprime(p) and isprime(p + 2)):
                p += 2
            self._memo.append(p)

        return self._memo[n - 1]


class PrimePiSequence(Sequence):
    def __init__(self):
        Sequence.__init__(self, 1, None)
        self._memo = [0]

    def __repr__(self):
        return "PrimePiSequence()"

    def _value(self, n):
        while len(self._memo) < n:
            nextvalue = self._memo[-1] + int(isprime(n))
            self._memo.append(nextvalue)

        return self._memo[n - 1]


class CountDivisorsSequence(Sequence):
    """ tau
    """
    def __init__(self):
        Sequence.__init__(self, 1, None)

    def __repr__(self):
        return "CountDivisorsSequence()"

    def _value(self, n):
        return count_divisors(n)


class SmallestDivisorSequence(Sequence):
    """ tau
    """
    def __init__(self):
        Sequence.__init__(self, 1, None)

    def __repr__(self):
        return "SmallestDivisorSequence()"

    def _value(self, n):
        return smallest_divisor(n)


class SumDivisorsSequence(Sequence):
    """
    """
    def __init__(self):
        Sequence.__init__(self, 1, None)

    def __repr__(self):
        return "SumDivisorsSequence()"

    def _value(self, n):
        return sum_divisors(n)


class EulerPhiSequence(Sequence):
    """ EulerPhi
    """
    def __init__(self):
        Sequence.__init__(self, 1, None)

    def __repr__(self):
        return "EulerPhiSequence()"

    def _value(self, n):
        return euler_phi(n)


class ExperimentalSequence(Sequence):
    def __init__(self):
        Sequence.__init__(self, 1, None)

    def __repr__(self):
        return "ExperimentalSequence()"

    def _value(self, n):
        return n % euler_phi(n)


class PolynomialSequence(Sequence):
    def __init__(self, first_index, last_index, coefficients, divisor):
        Sequence.__init__(self, first_index, last_index)
        self._coefficients = coefficients
        self._divisor = divisor

    def __repr__(self):
        return "PolynomialSequence({!r}, {!r})".format(self._first_index, self._coefficients)

    def _value(self, n):

        value = sum(self._coefficients[i] * (n ** i) for i in range(len(self._coefficients)))
        assert value % self._divisor == 0

        return value // self._divisor


class RecurrentSequence(Sequence):
    def __init__(self, first_index, last_index, initial_values, coefficients, k0):
        assert len(initial_values) == len(coefficients)
        Sequence.__init__(self, first_index, last_index)
        self._initial_values = initial_values
        self._coefficients   = coefficients
        self._k0             = k0

        self._memo = {}

    def __repr__(self):
        return "RecurrentSequence({!r}, {!r})".format(self._first_index, self._last_index, self._initial_values, self._coefficients, self._k0)

    def _value(self, n):

        if (n - self._first_index) < len(self._initial_values):
            return self._initial_values[n - self._first_index]

        if n in self._memo:
            return self._memo[n]

        value = sum(self._value(n - len (self._coefficients) + i) * self._coefficients[i] for i in range(len(self._coefficients))) + self._k0

        self._memo[n] = value

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


def read_catalog_files(glob_pattern):

    sequence_name_to_type = {
        "PolynomialSequence"      : PolynomialSequence,
        "RecurrentSequence"       : RecurrentSequence,
        "TreeCountSequence"       : TreeCountSequence,
        "CountDivisorsSequence"   : CountDivisorsSequence,
        "EulerPhiSequence"        : EulerPhiSequence,
        "PrimeSequence"           : PrimeSequence,
        "SumDivisorsSequence"     : SumDivisorsSequence,
        "PrimePiSequence"         : PrimePiSequence,
        "TwinPrimeSequence"       : TwinPrimeSequence,
        "SmallestDivisorSequence" : SmallestDivisorSequence
    }

    with start_timer() as timer:

        catalog = {}

        filenames = sorted(glob.glob(glob_pattern))

        for filename in filenames:

            logger.info("Fetching catalog data from file '{}' ...".format(filename))

            with open(filename) as f:
                oeis_catalog = json.load(f)

            for (oeis_id_string, sequence_name, sequence_args) in oeis_catalog:

                assert oeis_id_string.startswith("A")
                oeis_id = int(oeis_id_string[1:])

                if oeis_id in catalog:
                    logger.error("A{:06d} in file {!r} previously defined as {!r}, skipping.".format(oeis_id, filename, catalog[oeis_id]))
                    continue

                sequence_type = sequence_name_to_type[sequence_name]
                sequence = sequence_type(*sequence_args)

                catalog[oeis_id] = sequence

        catalog = OrderedDict(sorted(catalog.items()))

        logger.info("Fetched {} catalog entries in {}.".format(len(catalog), timer.duration_string()))

    return catalog
