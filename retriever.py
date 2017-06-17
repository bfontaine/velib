# -*- coding: UTF-8 -*-

import time
import logging
import argparse

import requests
import arrow

import db

db.init_db()

OUTDIR = "data"
URL = "https://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/download/?format=json&timezone=Europe/Berlin"

def retrieve():
    try:
        r = requests.get(URL)
    except requests.exceptions.ConnectionError:
        logging.error("protocol error")
        return False

    if not r.ok:
        logging.error("Cannot get data: %s" % r)
        return False

    db.save_disponibilities(r.json())

    logging.info("saved results from %s" % arrow.now())
    return True


def run():
    while True:
        while not retrieve():
            logging.debug("Trying again in 15 sec")
            time.sleep(15)
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
