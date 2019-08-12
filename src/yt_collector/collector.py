#!/usr/bin/env python
# coding: utf-8

"""
This file collects YouTube channel and video metadata 
using the `youtube_api` package. 

It expects as input a CSV file containing URLs to YouTube
channels in a column titled "url". The script also builds
a set of cache files to avoid unnecessary calls to the YT
API. 

For each URL in the provided CSV file, the script pulls
the metadata for the channel and its "upload playlist",
which contains all videos the channel has ever uploaded.

For each video in that list, the script pulls down
metadata about that video and stores it in JSON file. 

NOTE: You must also provide a valid YouTube API key in
the environment variable YT_KEY.

"""

import sys
import json
import requests
import os
import os.path

import time

import pandas as pd

from youtube_api import YouTubeDataAPI
from youtube_api import youtube_api_utils

def get_channel_from_url(url):
    """
    Given a URL to a YouTube channel, this function downloads
    the HTML for that URL using the `requests` package and 
    parses the YouTUbe channel ID from the collected HTML.
    """

    r = requests.get(url) 
    
    channel_id = None
    if r.status_code == 200:
        content = r.content.decode("utf8")

        # Look for the the URL to the YouTube channel in the meta data
        loc = content.find('<meta property="og:url" content="https://www.youtube.com/channel')

        # If the meta tag exists, take the content after the "/channel/" path
        #. as the channel ID
        if ( loc >= 0 ):
            channel_id = content[loc:].partition(">")[0].rpartition("/channel/")[-1].replace('"', "")
        else:
            print("ERROR on [%s]: Can't find channel marker..." % url)
    else:
        print("Error on [%s]:" % url, r)

    return channel_id

# Get the API key from the environment
api_key = os.environ.get('YT_KEY')
if ( api_key is None ):
    sys.stderr.write("ERROR! Need a YouTube API key in YT_KEY environment variable\n")
    sys.exit(-1)

# Create out Youtube API object
yt = YouTubeDataAPI(api_key)

# Make sure we have input
if ( len(sys.argv) == 1 ):
    sys.stderr.write("ERROR! Need an input CSV file\n")
    sys.stderr.write("USAGE: %s yt_channels.csv\n" % sys.argv[0])
    sys.exit(-1)

# Create a dataframe from our input
in_file = sys.argv[1]
df = pd.read_csv(in_file)

# Set up the cache file, so we know what URLs map to what channels
#. if this file exists, read it and update the map of URLs
id_cache_file = "url2channelId.csv"
found_url_map = {}
if os.path.exists(id_cache_file):
    local_df = pd.read_csv(id_cache_file, header=None)
    found_url_map = {tup[1]:tup[2] for tup in local_df.itertuples()}

# for each URL, get the channel ID
channel_ids = []
with open(id_cache_file, "w") as out_file:
    for url in df["url"]:

        channel_id = None

        # Have we cached this URL?
        if ( url in found_url_map ):
            channel_id = found_url_map[url]

        elif "/channel/" in url: # Can we pull it easily from ULR?
            channel_id = url.partition("/channel/")[-1].partition("?")[0]
        
        else: # For all other spaces, get channel from URL
            channel_id = get_channel_from_url(url)

        # IF we got a good channel ID, write it to the cache
        if channel_id is not None:
            out_file.write("%s,%s\n" % (url, channel_id))
        
            channel_ids.append(channel_id)

# Read cache file about channel metadata
channel_metadata_cache_file = "chan_meta.json"
channel_meta_map = {}
if os.path.exists(channel_metadata_cache_file):
    with open(channel_metadata_cache_file, "r") as in_file:
        for line in in_file:
            meta_obj = json.loads(line)
            channel_meta_map[meta_obj["id"]] = line.strip()

# For each channel;, get its upload playlist
upload_playlists = []
with open(channel_metadata_cache_file, "w") as out_file:
    for channel_id in channel_ids:
        print("Getting Channel Metadata:", channel_id)

        meta_obj = None

        # Check if we've cached this data
        if ( channel_id in channel_meta_map ):
            metadata = channel_meta_map[channel_id]
            meta_obj = json.loads(metadata)
        else:
            meta_obj = yt.get_channel_metadata(channel_id, parser=lambda x: x)
            meta_obj["minerva_collected"] = time.time()

        # Write data to cache file and add the upload list to 
        #. the map of data we'll pull
        out_file.write("%s\n" % json.dumps(meta_obj))
        uploads = meta_obj["contentDetails"]["relatedPlaylists"]["uploads"]
        upload_playlists.append((channel_id, uploads))

# we store channel data in a directory for each channel
if not os.path.exists("channels"):
    os.mkdir("channels")

# For each channel and its upload playlist, pull the videos
for channel_id, upload_playlist in upload_playlists:

    target_path = os.path.join("channels", channel_id)
    print("Capturing Videos for:", channel_id)

    if not os.path.exists(target_path):
        os.mkdir(target_path)
    
    videos_file = os.path.join(target_path, "playlist_videos.json")

    video_count = 0
    video_ids = []

    # Find every video in the upload playlist and write it to the 
    #. file for this playlist
    with open(videos_file, "w") as out_file:
        for v in yt.get_videos_from_playlist_id(upload_playlist, part=["snippet", ], parser=lambda x: x):
            v["minerva_collected"] = time.time()
            out_file.write("%s\n" % json.dumps(v))
            out_file.flush()

            video_ids.append(v["snippet"]["resourceId"]["videoId"])
            video_count += 1
    print("\t Captured playlist videos:", video_count)

    # Now we test our cache to see what videos we've already pulled
    video_ids_to_capture = []
    for v in video_ids:
        this_v_file = os.path.join(target_path, "%s.json" % v)
        if not os.path.exists(this_v_file):
            video_ids_to_capture.append(v)

    # For all videos in this playlist, download their metadata
    slice_size = 50
    for i in range(0, len(video_ids_to_capture), slice_size):
        v_ids = video_ids_to_capture[i:i+slice_size]
        vid_data = yt.get_video_metadata(v_ids, part=['statistics','snippet','contentDetails'], parser=lambda x: x)

        for v in vid_data:
            v["minerva_collected"] = time.time()
            this_v_file = os.path.join(target_path, "%s.json" % v["id"])
            with open(this_v_file, "w") as out_file:
                out_file.write("%s" % json.dumps(v))









