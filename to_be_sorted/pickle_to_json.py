#! /usr/bin/env python3

import pickle
import json

with open("oeis.pickle", "rb") as f:
    entries = pickle.load(f)

# Note that we convert the 'value' integers to strings; this prevents them from being treated as limited-precision
# numbers when the JSON representation is interpreted.

data = [(entry.oeis_id, entry.name, entry.offset, [str(v) for v in entry.values]) for entry in entries]

with open("oeis.json", "w") as f:
    json.dump(data, f)
