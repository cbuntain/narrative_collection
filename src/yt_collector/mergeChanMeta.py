#!/usr/bin/env python
# coding: utf-8

import sys
import copy
import glob
import gzip
import json
import math


original_data_map = {}
updated_data_map = {}

with open(sys.argv[1], "r") as in_file:
    for line in in_file:
        channel_obj = json.loads(line)

        channel_id = channel_obj["id"]
        original_data_map[channel_id] = channel_obj

with open(sys.argv[2], "r") as in_file:
    for line in in_file:
        channel_obj = json.loads(line)

        channel_id = channel_obj["id"]
        updated_data_map[channel_id] = channel_obj


for channel_id, new_chan_obj in updated_data_map.items():
    if channel_id not in original_data_map:
        print("No old channel obj for:", channel_id)
        continue

    old_chan_obj = original_data_map[channel_id]
    if ( "customUrl" in old_chan_obj["snippet"] ):
        new_chan_obj["snippet"]["customUrl"] = old_chan_obj["snippet"]["customUrl"]


with open(sys.argv[3], "w") as out_file:
    for channel_id, new_chan_obj in updated_data_map.items():
        out_file.write(json.dumps(new_chan_obj) + "\n")
        
