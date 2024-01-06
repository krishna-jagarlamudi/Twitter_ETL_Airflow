import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():

    access_key = "ZVnqn4L80iWdUDPBpjq5kBIqM" 
    access_secret = "VCY0KSVHqYQtbHmIpC38d6NMh3CMExlPljhVeiiuhbXE7LqkYj" 
    consumer_key = "1050812790875729920-MbXgRmc4Hm5X1RR3RDoAcGoe1a2IrZ"
    consumer_secret = "7jUN60AqdfYXEkLjYPw8aYl8lkj8FdcVQjItwzwAk0lXF"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@imVkohli', 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('refined_VK_tweets.csv')
