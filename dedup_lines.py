# -*- coding: UTF-8 -*-

import json
import gzip
import argparse
from cachetools import LRUCache

def doc_key(d):
    return "%d#%s#%d#%d" % (
        d["station_id"], d["last_update"], d["available_bike_stands"],
        d["available_bikes"])

def deduplicate(fr, fw):
    cache = LRUCache(maxsize=4000000)

    for line in fr:
        doc = json.loads(line)
        k = doc_key(doc)
        seen = cache.get(k, None)
        if seen:
            continue

        cache[k] = True
        fw.write(line)


def main():
    p = argparse.ArgumentParser(description="Custom JSON lines deduplicater")
    p.add_argument("infile")
    p.add_argument("outfile")
    args = p.parse_args()

    with gzip.open(args.infile, "rt", encoding="utf-8") as fr:
        with gzip.open(args.outfile, "wt", encoding="utf-8") as fw:
            deduplicate(fr, fw)


if __name__ == "__main__":
    main()
