#! /usr/bin/env python3

from __future__ import print_function
from catalog import read_catalog_files
import pickle

def main():

    catalog = read_catalog_files("catalog_files/*.json")
    print("catalog has {} entries.".format(len(catalog)))

    filename = "oeis_v20150915.pickle"
    with open(filename, "rb") as f:
        oeis_entries = pickle.load(f)

    print("pickled OEIS database has {} entries.".format(len(oeis_entries)))

    for (oeis_id, sequence) in catalog.items():

        if not (0 <= oeis_id - 1 < len(oeis_entries)):
            print("[A{:06d}] not found in OEIS pickle database, skipping ...".format(oeis_id))
            continue

        oeis_entry = oeis_entries[oeis_id - 1]

        assert sequence.first_index == oeis_entry.offset[0]

        n = min(100, len(oeis_entry.values))

        #print("[A{:06d}] Verifying catalog entry, {} of {} values ...".format(oeis_id, n, len(oeis_entry.values)))

        good = all(sequence[sequence.first_index + i] == oeis_entry.values[i] for i in range(n))
        assert good

        if any(k.startswith("f") for k in oeis_entry.keywords):
            print("[A{:06d}] Finite sequence: {}".format(oeis_id, oeis_entry.keywords))

if __name__ == "__main__":
    main()
