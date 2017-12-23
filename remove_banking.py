# -*- coding: UTF-8 -*-

import json
import gzip
import argparse

argparse.ArgumentParser().parse_args()

def load_stations():
    with open("d/stations.jsons") as fr:
        return [json.loads(line) for line in fr]

def save_stations(stations):
    with open("d/stations.jsons", "w") as fw:
        for s in stations:
            json.dump(s, fw, sort_keys=True)
            fw.write("\n")

def process_stations():
    stations = load_stations()

    # All stations have banking=true
    for s in stations:
        s["banking"] = True

    save_stations(stations)

def process_dispos():
    with gzip.open("d/dispos-all.jsons.gz", "rt", encoding="utf-8") as fr:
        with gzip.open("d/dispos-all-without-banking.jsons.gz", "wt",
                encoding="utf-8") as fw:

            for line in fr:
                doc = json.loads(line)
                doc.pop("banking")
                json.dump(doc, fw, sort_keys=True)
                fw.write("\n")


print("1- stations")
process_stations()
print("2- dispos")
process_dispos()
