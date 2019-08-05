
# coding: utf-8

# # Collect Link and Video Shares of Politicians on Facebook
# 
# We use the CrowdTangle API to collect links shared to websites and YouTube 
# videos by politicians on Facebook. We use the social media accounts from 
# a list of accounts provided by other researchers in the group.
# 
# __NOTE__: To use CrowdTangle, you must have created a list in CT with the 
# pages/groups about which you care. Then you use the API to download data 
# from that page/group.

import time
import json
import requests

import pandas as pd

domain = "api.crowdtangle.com"

token = os.environ.get('CT_KEY')
if ( token is None ):
    sys.stderr.write("ERROR! Need a CrowdTangle API key in CT_KEY environment variable\n")
    sys.exit(-1)

for year in [
    "2019", 
    "2018", 
    "2017", 
    "2016", 
    "2015", 
    "2014", 
]:

    print("Year:", year)

    # This request format is dictated by the CrowdTangle API
    req = requests.get("https://" + domain + "/posts", params={
        "token": token,
        "listIds": "1209491",
        "startDate": "%s-01-01" % year,
        "endDate": "%d-01-01" % (int(year)+1),
        "sortBy": "date",
        "count": 100, # Can only get ~100 items at a time
        "types": "photo,status,tweet,link,youtube",
    })

    response = req.json()

    # Create a new file for each year
    with open("politicians_text_%s.json" % year, "w") as out_file:
        while "result" in response:

            print("Result Length:", len(response["result"]["posts"]))
            
            # Write every response to the file
            for post in response["result"]["posts"]:
                out_file.write("%s\n" % json.dumps(post))

            # The CT API uses paging, so we have to iterate through
            #. the pages of data. Note we could hit the API rate limit
            #. during this process, so we use try-except to handle
            #. this case and wait for a minute before retrying
            if ( "nextPage" in response["result"]["pagination"] ):
                out_file.flush()
                next_page = response["result"]["pagination"]["nextPage"]

                req = requests.get(next_page)
                
                try:
                    response = req.json()
                except Exception as e:
                    print("Error:", e)
                    response = {"status": 429}
                    
                if ( response["status"] == 429 ):
                    time.sleep(60)

                    req = requests.get(next_page)
                    response = req.json()
            else:
                break



