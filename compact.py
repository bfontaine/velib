#! /usr/bin/env python
# -*- coding: UTF-8 -*-

import json
import gzip

with gzip.open("d/dispos-all.jsons.gz", "rt", encoding="utf-8") as fr:
    with gzip.open("d/dispos-compact.jsons.gz", "wt",
            encoding="utf-8") as fw:

        for line in fr:
            doc = json.loads(line)

            doc["bikes"] = doc.pop("available_bikes")
            doc["bike_stands"] = doc.pop("available_bike_stands")

            json.dump(doc, fw, sort_keys=True, separators=(',', ':'))
            fw.write("\n")
