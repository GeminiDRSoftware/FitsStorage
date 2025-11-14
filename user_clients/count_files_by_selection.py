#!/usr/bin/env python

import requests
import datetime
from argparse import ArgumentParser

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--server", action="store",
                        default="https://archive.gemini.edu",
                        help="server to query")
    parser.add_argument("--selection", action="store",
                        help="selection to query")
    args = parser.parse_args()

    if args.selection is None:
        print("Must supply selection at least")
        exit(1)

    url = '/'.join([args.server, "jsonsummary", args.selection])
    results = requests.get(url).json()

    tally = {}
    for result in results:
        d = datetime.datetime.fromisoformat(result['ut_datetime']).date()
        if d in tally:
            tally[d] += 1
        else:
            tally[d] = 1

    ds = list(tally.keys())
    ds.sort()
    for d in ds:
        print(f"{d} {d.strftime("%Y%m%d")} : {tally[d]}")
