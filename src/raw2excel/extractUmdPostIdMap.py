#!/usr/bin/env python
# coding: utf-8

import sys
import copy
import glob
import gzip
import json

import pandas as pd

if len(sys.argv) < 2:
    print("Usage: %s <in_file.csv> [in_file.csv]" % sys.argv[0])
    sys.exit()

map_platform_post_id_to_umd_post_id = {}

platform_id_col = "PlatformPostID"
umd_id_col = "UmdPostId"

for in_file_path in sys.argv[1:]:
    print("Extracting from:", in_file_path)

    ldf = pd.read_csv(in_file_path)

    for tup in ldf[[platform_id_col, umd_id_col]].itertuples():
        ppid = tup[1]
        umdid = tup[2]

        # Convert to ints
        try:
            umdid = int(umdid)
            map_platform_post_id_to_umd_post_id[ppid] = umdid
        except:
            pass

print("Writing map to file...")
with open("extracted_post_id_map.json", "w") as out_file:
    json.dump(map_platform_post_id_to_umd_post_id, out_file)

