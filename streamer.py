#!/usr/bin/env python3

import tweepy
import requests
import json
import logging
from datetime import datetime
import time
import sys
import os
from functools import wraps

consumer_key = os.environ["TWITTERELK_CONSUMER_KEY"]
consumer_secret = os.environ["TWITTERELK_CONSUMER_SECRET"]
access_token = os.environ["TWITTERELK_ACCESS_TOKEN"]
access_token_secret = os.environ["TWITTERELK_ACCESS_TOKEN_SECRET"]
elastic_search_api_base = os.environ["TWITTERELK_ELASTIC_SEARCH_API_BASE"]
elastic_auth = requests.auth.HTTPBasicAuth(
    os.environ["TWITTERELK_ELASTIC_SEARCH_USERNAME"], 
    os.environ["TWITTERELK_ELASTIC_SEARCH_PASSWORD"],
)

round_wait_second = 360
fetch_max = 500
request_timeout_secs = 3
requests.adapters.DEFAULT_RETRIES = 5

logger = logging.getLogger(__name__)
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
def retry(tries=4, delay=3, backoff=2):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except Exception as ex:
                    logger.warning(f"{ex}, Retrying in {mdelay} seconds...")
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry  # true decorator
    return deco_retry

def create_index(elastic_type):
    resp = requests.put(f"{elastic_search_api_base}/{elastic_type}",
        headers={
            "Content-Type": "application/json"
        },
        auth=elastic_auth,
        data=json.dumps({
            "settings": {
                "index.mapping.total_fields.limit": 1500,
            },
            "mappings": {
                    "properties": {
                    "created_timestamp": { "type": "date_nanos" }
                }
            }
        }),
        timeout=request_timeout_secs,
    )
    logger.info(resp.text)

def config_index(elastic_type):
    resp = requests.put(f"{elastic_search_api_base}/{elastic_type}/_settings",
        headers={
            "Content-Type": "application/json"
        },
        auth=elastic_auth,
        data=json.dumps({
            "index.mapping.total_fields.limit": 1500,
        }),
        timeout=request_timeout_secs,
    )
    logger.info(resp.text)

@retry()
def upload_tweet(tweet, elastic_type):
    data = tweet._json
    t = datetime.strptime(data['created_at'], '%a %b %d %H:%M:%S %z %Y')
    data['created_timestamp'] = t.timestamp() * 1000
    resp = requests.post(f"{elastic_search_api_base}/{elastic_type}/_doc/{tweet.id}",
        headers={
            "Content-Type": "application/json"
        },
        auth=elastic_auth,
        data=json.dumps(data),
        timeout=request_timeout_secs,
    )
    logger.info(f"{resp.status_code} => {tweet.id}@{tweet.user.screen_name}: {tweet.full_text[:20]}")
    if resp.status_code != 201:
        logger.error(resp.text)
    else:
        logger.info(resp.text)

def get_last_known_id(elastic_type):
    resp = requests.get(f"{elastic_search_api_base}/{elastic_type}/_doc/_search",
        headers={
            "Content-Type": "application/json"
        },
        auth=elastic_auth,
        data=json.dumps({
            "_source": ["_id"],
            "query": {
                "match_all": {}
            },
            "size": 1,
            "sort": [ 
                {"_id":{"order": "desc"}}
            ],
        }),
        timeout=request_timeout_secs,
    )
    resp.raise_for_status()
    ret = int(resp.json()['hits']['hits'][0]['_id'])
    logger.info(f"Last known id for type {elastic_type} is {ret}")
    return ret


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s|%(name)-6s[%(levelname)s] %(message)s'
    )
    
    create_index("twitter")
    create_index("twitter_mentions")

    config_index("twitter")
    config_index("twitter_mentions")

    last_timeline_id = get_last_known_id("twitter")
    last_mention_id = get_last_known_id("twitter_mentions")
    while(True):
        try:
            # fetch home timeline
            for tweet in tweepy.Cursor(api.home_timeline, tweet_mode='extended', since_id=last_timeline_id).items(fetch_max):
                last_timeline_id = max(last_timeline_id, tweet.id)
                upload_tweet(tweet, "twitter")

        except tweepy.error.TweepError as ex:
            logger.exception(ex)

        try:
            # fetch mention timeline
            for tweet in tweepy.Cursor(api.mentions_timeline, tweet_mode='extended', since_id=last_mention_id).items(fetch_max):
                last_mention_id = max(last_mention_id, tweet.id)
                upload_tweet(tweet, "twitter_mentions")

        except tweepy.error.TweepError as ex:
            logger.exception(ex)
        
        logger.info("Wait a while")
        time.sleep(round_wait_second)
