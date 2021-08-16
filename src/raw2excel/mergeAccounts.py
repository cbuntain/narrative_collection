#!/usr/bin/env python
# coding: utf-8

import copy
import glob
import gzip
import json
import math

import pandas as pd

from datetime import datetime

country = "PL"

fb_accts_df = pd.read_csv("fb_structure_accounts.csv", encoding="utf8")
yt_accts_df = pd.read_csv("yt_structure_accounts.csv", encoding="utf8")

yt_accts_indexed_df = yt_accts_df.set_index("UmdAccountID")
fb_accts_indexed_df = fb_accts_df.set_index("UmdAccountID")


def create_account_row():
    row_schema = {
        "UmdAccountID": None,
        "AccountPlatformId": None,
        "AccountName": None,
        "AgeofAccount": None,
        "ChannelCreateDate": None,
        "FBFriends": None,
        "YTSubscribers": None,
        "ChannelVideoCount": None,
        "FBPostCount": None,
        "ChannelViewCount": None,
        "FBAllReactions": None,
        "AccountDataCountry": None,
        "Followers_Parent": None,
        "Followers_Child": None,
        "Following_Parent": None,
        "Following_Child": None,
        "NumberOfTweets_Parent": None,
        "NumberOfTweets_Child": None,
        "Verified_Parent": None,
        "Verified_Child": None,
        
        # CB Added 20190909
        "FBLikes": None,
        "FBComments": None,
        "FBShares": None,
        "Verified": None,
        "TimestampDownload": None,
    }
    
    return row_schema

all_umd_ids = set(fb_accts_indexed_df.index).union(yt_accts_indexed_df.index)

print("Accounts:", len(all_umd_ids))

full_merged_df = fb_accts_indexed_df.join(yt_accts_indexed_df, how="outer", lsuffix="_fb", rsuffix="_yt")

new_rows = []

for idx, row in full_merged_df.to_dict("index").items():
   
    new_row = create_account_row()
    
    merge_cols = list(new_row.keys())
    merge_cols.remove("UmdAccountID")
    merge_cols.remove("AccountPlatformId")
    
    new_row["UmdAccountID"] = idx
    new_row["AccountPlatformId_fb"] = row["AccountPlatformId_fb"]
    new_row["AccountPlatformId_yt"] = row["AccountPlatformId_yt"]
    
    for c in merge_cols:
        new_row[c] = row[c + "_yt"]
        if new_row[c] is None or (type(new_row[c]) == float and math.isnan(new_row[c])):
            new_row[c] = row[c + "_fb"]

    new_rows.append(new_row)


final_acct_df = pd.DataFrame(new_rows).set_index("UmdAccountID")
final_acct_df.to_csv("merged_accounts.csv", index=True)

