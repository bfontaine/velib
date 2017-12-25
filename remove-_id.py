#! /usr/bin/env python
# -*- coding: UTF-8 -*-

import json
import gzip

with gzip.open("d/dispos-all.jsons.gz", "rt", encoding="utf-8") as fr:
    with gzip.open("d/dispos-all-without-_id.jsons.gz", "wt",
            encoding="utf-8") as fw:

        for line in fr:
            doc = json.loads(line)
            doc.pop("_id")
            json.dump(doc, fw, sort_keys=True)
            fw.write("\n")
