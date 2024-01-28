#!/usr/bin/python3

import json
import os
import re
import zipfile

def extract(filename):
    """ Extract a single zipped json file into individual files per caseNumber """
    print("extracting", filename)
    item = re.search("_(.*?).zip$", filename).group(1)
    with zipfile.ZipFile(filename, "r") as z:
        assert len(z.namelist()) == 1
        with z.open(z.namelist()[0]) as f:
            d = json.loads(f.read())
            for case in d:
                dest = f'{item}/{case["caseNumber"]}.json'
                save_json(dest, case)

def save_json(dest, data):
    if not os.path.exists(dest):
        # In Windows 10, python3 you need to specify encoding
        with open(dest, "w", encoding="utf-8") as outfile:
            json.dump(data, outfile, indent=4, sort_keys=True, ensure_ascii=False)

def extract_all():
    for filename in sorted(os.listdir("zipcache")):
        extract("zipcache/" + filename)

for d in ["jo", "h2a", "h2b"]:
# for d in ["jo"]:
    if not os.path.exists(d):
        os.mkdir(d)

extract_all()
