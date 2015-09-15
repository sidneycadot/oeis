#! /usr/bin/env python3

def memoize(f):
    memo = {}
    def inner(*args):
        if args in memo:
            return memo[args]
        result = f(*args)
        memo[args] = result
        return result
    return inner

def make_sequence_string(sequence, first_index, last_index = None, max_chars = None):
    assert (last_index is not None) or (max_chars is not None)
    s = ""
    i = first_index
    while True:
        if last_index is not None and i == last_index:
            break
        next_value = sequence(i)
        if s == "":
            next_s = str(next_value)
        else:
            next_s = s + ", " + str(next_value)
        if max_chars is not None and len(next_s) > max_chars:
            break
        s = next_s
        i += 1
    return s

@memoize
def count_trees(n, m, split):
    """ We must assign 'n' leaves.
        At the current level of the tree, 'm' nodes are open.
        Each of the 'm' nodes must either be split up, or assigned.
    """

    if n == m:
        return 1

    if n < m:
        return 0

    # We know now that n > m.

    if m == 0:
        return 0

    return sum(count_trees(n -  i, (m - i) * split, split) for i in range(m + 1))

oeis_generators = {
        "A002572": (lambda n : count_trees(1 + ( 2 - 1) * (n - 1), 1,  2), (1, None)),
        "A176485": (lambda n : count_trees(1 + ( 3 - 1) * (n - 1), 1,  3), (1, None)),
        "A176503": (lambda n : count_trees(1 + ( 4 - 1) * (n - 1), 1,  4), (1, None)),
        "A194628": (lambda n : count_trees(1 + ( 5 - 1) * (n - 1), 1,  5), (1, None)),
        "A194629": (lambda n : count_trees(1 + ( 6 - 1) * (n - 1), 1,  6), (1, None)),
        "A194630": (lambda n : count_trees(1 + ( 7 - 1) * (n - 1), 1,  7), (1, None)),
        "A194631": (lambda n : count_trees(1 + ( 8 - 1) * (n - 1), 1,  8), (1, None)),
        "A194632": (lambda n : count_trees(1 + ( 9 - 1) * (n - 1), 1,  9), (1, None)),
        "A194633": (lambda n : count_trees(1 + (10 - 1) * (n - 1), 1, 10), (1, None))
    }

def main():
    MAX_CHARS  = 200
    LAST_INDEX = None
    for key in sorted(oeis_generators.keys()):
        (sequence, (first_index, last_index)) = oeis_generators[key]
        print("{}: {}".format(key, make_sequence_string(sequence, first_index, last_index = LAST_INDEX, max_chars = MAX_CHARS)))

if __name__ == "__main__":
    main()
