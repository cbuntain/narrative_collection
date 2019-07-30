#!/usr/bin/env python
# coding: utf-8


"""
This file takes a CSV file of Facebook URLs and 
converts them to a format that CrowdTangle can 
understand in its bulk upload.

"""


import pandas as pd


# Make sure we have input
if ( len(sys.argv) < 3 ):
    sys.stderr.write("ERROR! Need an input CSV file and list name\n")
    sys.stderr.write("USAGE: %s fb_pages.csv '<CrowdTangle List>'\n" % sys.argv[0])
    sys.exit(-1)


df = pd.read_csv(sys.argv[1], index_col=0)


# We need to know what list we're uploading to
df["List"] = sys.argv[2]

# Rename columns and write to file
df.rename(columns={"url": "Page or Account URL"})\
    .to_csv("fb_crowdtangle.csv", index=False)


