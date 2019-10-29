#!/usr/bin/env python
# coding: utf-8

# In[1]:


import copy
import glob
import gzip
import json
import math

import pandas as pd

from datetime import datetime


# In[ ]:





# In[2]:


country = "LT"


# In[ ]:





# In[3]:


coordination_df = pd.read_excel("../../data/lithuania/LT influencers Oct 29 2019.xlsx")


# In[4]:


fb_df = pd.read_csv("../../data/lithuania/fb_pages.csv", encoding="utf8")
yt_df = pd.read_csv("../../data/lithuania/yt_channels.csv", encoding="utf8")
tw_df = pd.read_csv("../../data/lithuania/tw_handles.csv", encoding="utf8")
ig_df = pd.read_csv("../../data/lithuania/insta_pages.csv", encoding="utf8")


# In[ ]:





# In[5]:


fb_collection_path = "../../data/lithuania/collections/fb/*.gz"
yt_collection_path = "../../data/lithuania/collections/yt/"


# In[ ]:





# In[6]:


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

def create_post_row():
    row_schema = {
        "AccountPlatformId": None,
        "PostID": None,
        "PlatformPostID": None,
        "URL": None,
        "VideoTitle": None,
        "Text": None,
        "Platform": None,
        "TimestampDownload": None,
        "TimestampPosted": None,
        "TimeBtwnPostData": None,
        "YTViews": None,
        "YTLikes": None,
        "FBLikes": None,
        "YTDislikes": None,
        "FBShares": None,
        "YTShares": None,
        "FBComments": None,
        "YTComments": None,
        "FBReactionsTotal": None,
        "FBrxHeart": None,
        "FBrxHaHa": None,
        "FBrxWow": None,
        "FBrxSad": None,
        "FBrxAngry": None,
        "LengthofYTVid": None,
        "TweetType": None,
        "TweetID_Parent": None,
        "TweetID_Child": None,
        "Timestamp_Parent": None,
        "Timestamp_Child": None,
        "Username_Parent": None,
        "Username_Child": None,
        "Text_Parent": None,
        "Text_Child": None,
        "RetweetCount_Parent": None,
        "RetweetCount_Child": None,
        "FavoritedCount_Parent": None,
        "FavoritedCount_Child": None,
        "RefreshTime": None,
        
        # CB Added 20190909
        "PostType": None,
        "FBrxThankful": None,
    }
    
    return row_schema


# In[ ]:





# # Facebook Data
# 
# 

# In[ ]:





# In[7]:


account_map_fb = {}
fb_rows = []

for dataset in glob.iglob(fb_collection_path):
    with gzip.open(dataset, "r") as in_file:
        for line_ in in_file:
            line = line_.decode("utf8")
            
            fb_post = json.loads(line)
            
            # Process the author
            post_author = fb_post["account"]
            
            if "https://facebook.com/null" == post_author["url"]:
                print("Null FB URL")
                print(post_author)
                continue
            
            fb_author_id = None
            if "platformId" in post_author:
                fb_author_id = post_author["platformId"]
            else:
                fb_author_id = post_author["handle"]
            
            if fb_author_id not in account_map_fb:
                post_author["TimestampDownload"] = datetime.strptime(
                    fb_post["updated"], "%Y-%m-%d %H:%M:%S")
                
                post_author["FBPostCount"] = 0
                post_author["FBComments"] = 0
                post_author["FBLikes"] = 0
                post_author["FBShares"] = 0
                post_author["FBAllReactions"] = 0
                account_map_fb[fb_author_id] = post_author
            else:
                post_author = account_map_fb[fb_author_id]
                
            # Process the post
            this_post_row = create_post_row()
            
            # Top-level info
            this_post_row["AccountPlatformId"] = fb_author_id
            this_post_row["PlatformPostID"] = fb_post["platformId"]
            this_post_row["URL"] = fb_post["postUrl"]
            
            # Get type
            this_post_type = fb_post["type"]
            this_post_row["PostType"] = this_post_type
            
            # Get text of post
            this_post_text = ""
            if ( "title" in fb_post ):
                this_post_text += " " + fb_post["title"]
                
            if ( "message" in fb_post ):
                this_post_text += " " + fb_post["message"]

            if ( "description" in fb_post ):
                this_post_text += " " + fb_post["description"]
                
            if ( "expandedLinks" in fb_post ):
                for l in fb_post["expandedLinks"]:
                    this_post_text += " " + l["expanded"]
                
            if ( "media" in fb_post ):
                for l in fb_post["media"]:
                    this_post_text += " " + l["url"]
            
            this_post_row["Text"] = this_post_text
            
            # Get statistics of post
            stat_block = fb_post["statistics"]["actual"]
            this_post_row["FBrxHeart"] = stat_block["loveCount"]
            this_post_row["FBrxHaHa"] = stat_block["hahaCount"]
            this_post_row["FBrxWow"] = stat_block["wowCount"]
            this_post_row["FBrxSad"] = stat_block["sadCount"]
            this_post_row["FBrxAngry"] = stat_block["angryCount"]
            this_post_row["FBrxThankful"] = stat_block["thankfulCount"]
            this_post_row["FBComments"] = stat_block["commentCount"]
            this_post_row["FBShares"] = stat_block["shareCount"]
            this_post_row["FBLikes"] = stat_block["likeCount"]
            
            reactions_total = 0
            for k, v in this_post_row.items():
                if k.startswith("FBrx"):
                    reactions_total += v
            this_post_row["FBReactionsTotal"] = reactions_total
            
            # post times
            this_post_row["TimestampPosted"] = fb_post["date"]
            this_post_row["TimestampDownload"] = fb_post["updated"]
            
            this_download_time = datetime.strptime(fb_post["updated"], "%Y-%m-%d %H:%M:%S")
            if post_author["TimestampDownload"] < this_download_time:
                post_author["TimestampDownload"] = this_download_time
            
            # Increment account-level stats
            post_author["FBPostCount"] += 1
            post_author["FBComments"] += stat_block["commentCount"]
            post_author["FBLikes"] += stat_block["likeCount"]
            post_author["FBShares"] += stat_block["shareCount"]
            post_author["FBAllReactions"] += reactions_total
            
            # Done
            fb_rows.append(this_post_row)


# In[ ]:





# In[8]:


fb_platform_to_umdid_map = {
    row[2].replace("https://www.facebook.com/", "").partition("/")[0].lower():row[1] 
    for row in fb_df.itertuples()
}


# In[9]:


# fb_accts_df
fb_accounts = []

for acct_id, acct_map in account_map_fb.items():

    # Need to fix several errors in the way FB pages get named
    acct_handle = None
    if ( "platformId" in acct_map ):
        acct_handle = (acct_map["name"] + "-" + acct_map["platformId"])            .lower()            .replace(" ", "-")            .replace(".", "")            .replace(",", "")            .replace("---", "-").partition("/")[0].lower()
    if "handle" in acct_map:
        acct_handle = acct_map["handle"].lower()
        
    local_platform_id = None
    if ( "platformId" in acct_map ):
        local_platform_id = acct_map["platformId"]
    else:
        local_platform_id = acct_handle
        print("No platform ID:", acct_map)
    
    this_account = create_account_row()
    
    if acct_handle in fb_platform_to_umdid_map:
        this_account["UmdAccountID"] = fb_platform_to_umdid_map[acct_handle]
        this_account["AccountPlatformId"] = local_platform_id
        this_account["AccountName"] = acct_map["name"]
        this_account["FBFriends"] = acct_map["subscriberCount"]
        this_account["FBPostCount"] = acct_map["FBPostCount"]
        this_account["FBAllReactions"] = acct_map["FBAllReactions"]
        this_account["FBComments"] = acct_map["FBComments"]
        this_account["FBLikes"] = acct_map["FBLikes"]
        this_account["FBShares"] = acct_map["FBShares"]
        this_account["Verified"] = acct_map["verified"]
        this_account["TimestampDownload"] = acct_map["TimestampDownload"].strftime("%Y-%m-%d %H:%M:%S")

        this_account["AccountDataCountry"] = country

        fb_accounts.append(this_account)
    else:
        print("Failed to find matching UMD ID:", acct_handle)
        print(acct_map)


# In[ ]:





# In[10]:


fb_posts_df = pd.DataFrame(fb_rows)


# In[11]:


fb_accts_df = pd.DataFrame(fb_accounts)


# In[ ]:





# In[12]:


fb_posts_df.to_csv("fb_structure_posts.csv", index=False, encoding="utf8")
fb_accts_df.to_csv("fb_structure_accounts.csv", index=False, encoding="utf8")


# In[ ]:





# # YouTube Data

# In[ ]:





# In[13]:


yt_platform_to_umdid_map = {
    row[2].rpartition("/")[-1].lower():row[1] 
    for row in yt_df.itertuples()
}


# In[14]:


found_channels = set()

yt_channels = {}
with open(yt_collection_path + "/chan_meta.json") as in_file:
    for line in in_file:
        yt_channel = json.loads(line)
        
        this_channel = create_account_row()
        
        this_channel_id = yt_channel["id"]
        this_channel_umd_id = None
        
        # Find the UMD account ID for this channel
        if this_channel_id.lower() in yt_platform_to_umdid_map:
            this_channel_umd_id = yt_platform_to_umdid_map[this_channel_id.lower()]
        else:
            if ( "customUrl" in yt_channel["snippet"] ):
                custom_url = yt_channel["snippet"]["customUrl"]
                
                if custom_url.lower() in yt_platform_to_umdid_map:
                    this_channel_umd_id = yt_platform_to_umdid_map[custom_url.lower()]
        if ( this_channel_umd_id is None ):
            print("FATAL ERROR", this_channel_id)
            continue
            
        
        # Set the UMD ID
        this_channel["UmdAccountID"] = this_channel_umd_id

        this_channel["AccountName"] = yt_channel["snippet"]["title"]
        this_channel["AccountPlatformId"] = yt_channel["id"]
        this_channel["ChannelVideoCount"] = yt_channel["statistics"]["videoCount"]
        this_channel["ChannelViewCount"] = yt_channel["statistics"]["viewCount"]
        this_channel["ChannelCommentCount"] = yt_channel["statistics"]["commentCount"]
        this_channel["YTSubscribers"] = yt_channel["statistics"]["subscriberCount"]
        this_channel["ChannelCreateDate"] = datetime            .strptime(yt_channel["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%S.000Z")            .strftime("%Y-%m-%d %H:%M:%S")
        this_channel["TimestampDownload"] = datetime            .fromtimestamp(yt_channel["minerva_collected"])            .strftime("%Y-%m-%d %H:%M:%S")

        if ( "country" in yt_channel["snippet"] ):
            this_channel["AccountDataCountry"] = yt_channel["snippet"]["country"]
        else:
            this_channel["AccountDataCountry"] = country
        
        yt_channels[this_channel_id] = this_channel


# In[ ]:





# In[15]:


yt_accts_df = pd.DataFrame(list(yt_channels.values()))


# In[16]:


yt_accts_df.to_csv("yt_structure_accounts.csv", index=False, encoding="utf8")


# In[ ]:





# In[17]:


def duration_to_seconds(dur_str):
    """Convert YT's ISO timespans into seconds"""

    day = 0
    hour = 0
    minute = 0
    second = 0

    dur_str = dur_str.replace("PT", "")
    if ( "DT" in dur_str ):
        day, _, dur_str = dur_str.partition("DT")
    if ( "H" in dur_str ):
        hour, _, dur_str = dur_str.partition("H")
    if ( "M" in dur_str ):
        minute, _, dur_str = dur_str.partition("M")
    if ( "S" in dur_str ):
        second, _, dur_str = dur_str.partition("S")

    total_seconds = 24*60*60*int(day) + 60*60*int(hour) + 60*int(minute) + int(second)

    return total_seconds


# In[18]:



yt_videos = []

for video_path in glob.iglob(yt_collection_path + "/channels/*/*.json"):
    
    if ( "playlist" in video_path ):
        continue
    
    with open(video_path, "r") as in_file:
        video = json.load(in_file)

        # Process the video
        this_video_row = create_post_row()
        
        this_video_row["AccountPlatformId"] = video["snippet"]["channelId"]
        this_video_row["PlatformPostID"] = video["id"]
        this_video_row["URL"] = "https://www.youtube.com/watch?v=" + video["id"]

        # Title and description
        this_video_row["Text"] = video["snippet"]["description"]
        this_video_row["VideoTitle"] = video["snippet"]["title"]
        
        # Video statistics
        if ( "viewCount" in video["statistics"] ):
            this_video_row["YTViews"] = video["statistics"]["viewCount"]
        if ( "dislikeCount" in video["statistics"] ):
            this_video_row["YTDislikes"] = video["statistics"]["dislikeCount"]
        if ( "likeCount" in video["statistics"] ):
            this_video_row["YTLikes"] = video["statistics"]["likeCount"]
        if ( "commentCount" in video["statistics"] ):
            this_video_row["YTComments"] = video["statistics"]["commentCount"]

        # Get length
        this_video_row["LengthofYTVid"] =             duration_to_seconds(video["contentDetails"]["duration"])
        
        # Timing
        this_video_row["TimestampDownload"] = datetime            .fromtimestamp(video["minerva_collected"])            .strftime("%Y-%m-%d %H:%M:%S")
        
        this_video_row["TimestampPosted"] = datetime            .strptime(video["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%S.000Z")
        
        # Manually set the following
        this_video_row["Platform"] = "YouTube"
        this_video_row["PostType"] = "video"
  
        # Missing these
#         this_video_row["YTShares"] =

        yt_videos.append(this_video_row)


# In[ ]:





# In[19]:


yt_rows_df = pd.DataFrame(yt_videos)


# In[20]:


yt_rows_df.to_csv("yt_structure_posts.csv", index=False, encoding="utf8")


# In[ ]:





# # Merge Account Info

# In[21]:


yt_accts_indexed_df = yt_accts_df.set_index("UmdAccountID")
fb_accts_indexed_df = fb_accts_df.set_index("UmdAccountID")


# In[22]:


all_umd_ids = set(fb_accts_indexed_df.index).union(yt_accts_indexed_df.index)


# In[23]:


print("Accounts:", len(all_umd_ids))


# In[ ]:





# In[24]:


full_merged_df = fb_accts_indexed_df    .join(yt_accts_indexed_df, how="outer", lsuffix="_fb", rsuffix="_yt")


# In[25]:



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


# In[26]:


final_acct_df = pd.DataFrame(new_rows).set_index("UmdAccountID")


# In[28]:


final_acct_df.to_csv("merged_accounts.csv", index=True, encoding="utf8")


# In[ ]:




