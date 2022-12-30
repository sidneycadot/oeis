#! /usr/bin/env python3

import pickle

filename = "oeis_v20150920-10000.pickle"

print("Reading data ...")
with open(filename, "rb") as f:
    oeis_entries = pickle.load(f)
print("Done.")

print("Making candidate map ...")
candidate_map = {}
for (entry_index, oeis_entry) in enumerate(oeis_entries):
    for v in oeis_entry.values:
        if v not in candidate_map:
            candidate_map[v] = set()
        candidate_map[v].add(entry_index)
print("Done")

# A demo class to find

class FibLike:
    def __init__(self, first, weights = None, offset = 0, num_values = 100):
        n = len(first)
        if weights is None:
            weights = [1] * n
        assert len(first) == n
        values = [v for v in first]
        while len(values) < num_values:
            v = sum(weights[i] * values[len(values) - n + i] for i in range(n)) + offset
            values.append(v)
        self.values = values

# Find it.

fiblike = FibLike([1, 0], [1, 2], 1)
print(fiblike.values)

search1 = min(v for v in fiblike.values if v > 1000)
search2 = min(v for v in fiblike.values if v > 10000)
search3 = min(v for v in fiblike.values if v > 100000)

print([search1, search2, search3])

for candidate_index in sorted(candidate_map[search1] & candidate_map[search2] & candidate_map[search3]):
    oeis_entry = oeis_entries[candidate_index]
    print(oeis_entry, oeis_entry.name)
    print(oeis_entry.values)
    print()

