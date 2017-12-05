# -*- coding: UTF-8 -*-

import json
import argparse
from glob import glob

import db

def get_files():
    return sorted(glob("data/data/*.json"))

def concat_archive():
    files_dispos_sum = 0.0
    files_count = 0.0

    filenames = get_files()
    filenames_count = len(filenames)

    done_count = 0

    with open("data/additional.jsons", "w") as fw:
        for fname in filenames:
            with open(fname) as f:
                dispos = json.load(f)
                files_dispos_sum += len(dispos)
                files_count += 1.0

                factor = filenames_count / files_count
                total = files_dispos_sum * factor

                for dispo in dispos:
                    done_count += 1
                    json.dump(dispo, fw)
                    fw.write("\n")
                    percentage = (done_count / total) * 100.0
                    print((" [%3d%%] %d/%d\r" % (
                        percentage, done_count, total)),
                        end="")

        print()

def load_additional_dispos():
    filename = "data/additional.jsons"
    with open(filename) as f:
        for line in f:
            yield json.loads(line)

def main():
    #concat_archive()
    #return

    parser = argparse.ArgumentParser()
    parser.parse_args()

    db.save_disponibilities(load_additional_dispos(), batch=True)
    print("Done.")


if __name__ == "__main__":
    main()
