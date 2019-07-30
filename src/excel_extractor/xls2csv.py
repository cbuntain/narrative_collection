#!/usr/bin/env python
# coding: utf-8


"""
This file takes an Excel spreadsheet and 
extracts all FB, Insta, YT, and Twitter URLs
to individual CSV files for each platform

"""

import sys

import pandas as pd

from urllib.parse import urlparse

# Make sure we have input
if ( len(sys.argv) == 1 ):
    sys.stderr.write("ERROR! Need an input Excel file\n")
    sys.stderr.write("USAGE: %s input.xlsx\n" % sys.argv[0])
    sys.exit(-1)

inputPath = sys.argv[1]

df = pd.read_excel(inputPath, sheet_name="Influencers", encoding="UTF8")

# Platforms are specific columns in the sheet
platforms = [
    "FB",
    "YT",
    "Twitter",
    "Instagram",
]


# Make a map for each platform
platform_lists = {x:[] for x in platforms}

# For each row in this spreadsheet, parse the URLs for each platform
for row in df.to_dict("records"):
    for p in platforms:

        # Not all rows have valid URLs
        if row[p] is not None and isinstance(row[p], str) and row[p].lower().startswith("http"):
            
            this_url = row[p]
            if p == "FB" or p == "Instagram":
                o = urlparse(this_url)
                this_url = o.scheme + "://" + o.netloc + o.path
            elif p == "Twitter":
                o = urlparse(this_url)
                this_url = o.scheme + "://" + o.netloc + o.path
                this_url = this_url.replace("@", "").partition("/status")[0]
            elif p == "YT":
                if this_url.endswith("/"):
                    this_url = this_url[:-1]
            
            row_tuple = (row["ID"], this_url)
            platform_lists[p].append(row_tuple)
            

# How many elements do we have per platform?
for k, v in platform_lists.items():
    print(k, len(v))

# WRite out the FB data
fb_list = platform_lists["FB"]
fb_df = pd.DataFrame(fb_list, columns=["ID", "url"])
fb_df = fb_df.set_index("ID")

fb_df.to_csv("fb_pages.csv", encoding="UTF8")


# Write out the instagram data
insta_list = platform_lists["Instagram"]
insta_df = pd.DataFrame(insta_list, columns=["ID", "url"])
insta_df = insta_df.set_index("ID")

insta_df.to_csv("insta_pages.csv")

# Write out the YouTube data
yt_list = platform_lists["YT"]
yt_df = pd.DataFrame(yt_list, columns=["ID", "url"])
yt_df = yt_df.set_index("ID")

yt_df.to_csv("yt_channels.csv")

# Write out the Twitter data
tw_list = platform_lists["Twitter"]
tw_df = pd.DataFrame(tw_list, columns=["ID", "url"])
tw_df = tw_df.set_index("ID")

tw_df.to_csv("tw_handles.csv")

