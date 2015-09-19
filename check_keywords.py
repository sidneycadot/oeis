#! /usr/bin/env python3

import pickle
from collections import Counter

def make_pairs(keywords):
    return [(k1, k2) for k1 in keywords for k2 in keywords]

def main():
    filename = "oeis_v20150915.pickle"
    filename = "oeis_with_bfile.pickle"
    filename = "oeis_with_bfile-10000.pickle"

    with open(filename, "rb") as f:
        oeis_entries = pickle.load(f)

    pair_counter = Counter()

    all_keywords = set()
    for entry in oeis_entries:
        keywords = sorted(entry.keywords)
        all_keywords |= set(keywords)
        for pair in make_pairs(keywords):
            pair_counter[pair] += 1

    all_keywords = sorted(all_keywords)

    print("{:10}".format(""), end = "")
    for k2 in all_keywords:
        print("{:>10}".format(k2), end = "")
    print()

    for k1 in all_keywords:
        print("{:<10}".format(k1), end = "")
        for k2 in all_keywords:
            pair = (k1, k2)
            count = pair_counter[pair]
            print("{:>10}".format(count if count > 0 else ""), end = "")
        print()

if __name__ == "__main__":
    main()
