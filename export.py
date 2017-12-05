# -*- coding: UTF-8 -*-

import os
import argparse
import subprocess
from db import export_disponibilities, export_stations

# 20M
PAGE = 20 * 1000 * 1000

def export_page(n, filename):
    # 1:  0M-20M
    # 2: 20M-40M
    # 3: 40M-60M
    # ...
    offset = (n - 1) * PAGE
    limit = PAGE

    print("Page #%d -> %s" % (n, filename))

    export_disponibilities(offset, limit, filename)

    print("Copying on Storage...")
    subprocess.call(["gsutil", "-m", "cp", filename,
        "gs://bfontaine-cloud/velibs/disponibilities/"])
    print("Removing the local file...")
    os.unlink(filename)
    print("Done")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--stations", action="store_true")
    p.add_argument("page", type=int)
    args = p.parse_args()

    if args.stations:
        return export_stations("stations.jsons.gz")

    page = args.page
    export_page(page, "dispos-%02d.jsons.gz" % page)

if __name__ == "__main__":
    main()
