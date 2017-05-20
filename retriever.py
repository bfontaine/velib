# -*- coding: UTF-8 -*-

import os
import json
import time
import logging
import argparse

import requests
import arrow

OUTDIR = "data"
URL = "https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download/?format=json&timezone=Europe/Berlin"

def retrieve():
    os.makedirs(OUTDIR, exist_ok=True)
    r = requests.get(URL)
    if not r.ok:
        logging.error("Cannot get data: %s" % r)
        return

    now = arrow.now().timestamp
    filename = "%s/%s.json" % (OUTDIR, now)

    with open(filename, "w") as f:
        json.dump(r.json(), f, sort_keys=True, ensure_ascii=False)

    logging.info("wrote %s" % filename)


def run():
    while True:
        retrieve()
        logging.debug("sleeping for 60s")
        time.sleep(60)
        logging.debug("done sleeping")
    pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd", help="Command")
    opts = parser.parse_args()
    cmd = opts.cmd

    logging.basicConfig(filename="velib.log",
            format="%(asctime)s: %(levelname)s: %(message)s",
            level=logging.INFO)

    if cmd in {"r", "retrieve"}:
        retrieve()
    elif cmd in {"run",}:
        run()

if __name__ == "__main__":
    main()
