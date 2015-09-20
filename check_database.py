#! /usr/bin/env python3


# This page lists  constraints on the keywords:
#
#   http://oeis.org/wiki/User:Charles_R_Greathouse_IV/Keywords

import pickle
from collections import Counter

def make_pairs(keywords):
    return [(k1, k2) for k1 in keywords for k2 in keywords]

def digits(n):
    return len(str(abs(n)))

def main():

    filename = "oeis_v20150915.pickle"
    filename = "oeis_with_bfile.pickle"
    filename = "oeis_with_bfile-10000.pickle"
    filename = "oeis_v20150919.pickle"

    with open(filename, "rb") as f:
        oeis_entries = pickle.load(f)

    pair_counter = Counter()

    all_keywords = set()
    for entry in oeis_entries:
        keywords = sorted(entry.keywords)
        all_keywords |= set(keywords)
        for pair in make_pairs(keywords):
            pair_counter[pair] += 1

        oeis_id = entry.oeis_id

        max_digits = max(digits(v) for v in entry.values)
        if max_digits > 1000:
            print("A{:06d} max digits too large: {}".format(oeis_id, max_digits))
        
        if "tabl" in keywords and "tabf" in keywords:
            print("A{:06d} tabl/tabf together".format(oeis_id))
        if "nice" in keywords and "less" in keywords:
            print("A{:06d} nice/less together".format(oeis_id))
        if "easy" in keywords and "hard" in keywords:
            print("A{:06d} easy/hard together".format(oeis_id))
        if "nonn" in keywords and "sign" in keywords:
            print("A{:06d} nonn/sign together".format(oeis_id))
        if "full" in keywords and "more" in keywords:
            print("A{:06d} full/more together".format(oeis_id))

        if "allocated"  in keywords and len(keywords) > 1:
            print("A{:06d} allocated + more".format(oeis_id))
        if "allocating" in keywords and len(keywords) > 1:
            print("A{:06d} allocating + more".format(oeis_id))
        if "dead"       in keywords and len(keywords) > 1:
            print("A{:06d} dead + more".format(oeis_id))
        if "recycled"   in keywords and len(keywords) > 1:
            print("A{:06d} recycled + more".format(oeis_id))

        if not(("allocated" in keywords) or ("allocating" in keywords) or ("dead" in keywords) or ("recycled" in keywords)):
            if ("nonn" in keywords) == ("sign" in keywords):
                print("A{:06d} one of nonn, sign must be present.".format(oeis_id))
            if any(v < 0 for v in entry.values):
                if "sign" not in keywords:
                    print("A{:06d} negative values are present, but 'sign' keyword is missing.".format(oeis_id))


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
