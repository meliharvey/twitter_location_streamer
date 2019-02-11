# import packages to use
import sys
from datetime import timezone
import numpy as np
import pandas as pd
import tweepy

# create an account to get these keys and tokens
consumer_key = 'EP9URr20HSr4ekZ9LPfMf3eGn'
consumer_secret = 'Ehye5CVRd87QXExhbmeRvyvnOd6Qm8jhM1UeLdLNcloXDSaaNt'
access_token = '768411984-5uF4Mt9YQOy6lH6g4FWB8qNVPO46uQvTw0EJLB9W'
access_token_secret = 'v627MwIl7M5P2uP7QoQJ8AS8pywhiHSJG1NBfn3lVwIF2'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

location = [-74.29,40.46,-73.71,40.93] #[minimum longitude, minimum latitude, maximum longitude, maximum latitude]
has_coordinates = True # setting this to false will download all tweets within the area, even if they don't have specific coordinates

filename = 'tweets.csv'

tweet_counter = 0

# converting datetime to local time
def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)

#adding a tweet
def add_tweet(data):

    # define out counter as global
    global tweet_counter

    # counts the number of tweets extracted
    print(tweet_counter)

    #create dataframe
    df = pd.DataFrame()

    # create the fields and values
    df.loc[0,'id'] = data.id_str
    df.loc[0,'datetime'] = utc_to_local(data.created_at).strftime('%B %d, %Y %H:%M:%S')
    df.loc[0,'year'] = utc_to_local(data.created_at).year
    df.loc[0,'month'] = utc_to_local(data.created_at).month
    df.loc[0,'day_of_month'] = utc_to_local(data.created_at).day
    df.loc[0,'day_of_week'] = utc_to_local(data.created_at).weekday()
    df.loc[0,'hour'] = utc_to_local(data.created_at).hour
    df.loc[0,'minute'] = utc_to_local(data.created_at).minute
    df.loc[0,'second'] = utc_to_local(data.created_at).second
    df.loc[0,'text'] = data.text
    df.loc[0,'user_id'] = data.user.id
    df.loc[0,'name'] = data.user.name
    df.loc[0,'screen_name'] = data.user.screen_name
    df.loc[0,'follower_count'] = data.user.followers_count
    df.loc[0,'status_count'] = data.user.statuses_count
    df.loc[0,'lang'] = data.user.lang
    if has_coordinates == True:
        df.loc[0,'lat'] = data.coordinates['coordinates'][1]
        df.loc[0,'lng'] = data.coordinates['coordinates'][0]
    else:
        df.loc[0,'lat'] = None
        df.loc[0,'lng'] = None

    # if first tweet, write out a new file with headers
    if tweet_counter <= 0:
        df.to_csv(filename, sep=',', index=False, mode='w')

    # otherwise, append to existing file
    else:
        df.to_csv(filename, sep=',', index=False, header=None, mode='a')

    tweet_counter = tweet_counter + 1

#twitter stream
class CustomStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        if has_coordinates == False:
            add_tweet(status)
        elif status.coordinates == None:
            pass
        elif status.coordinates['type'] == 'Point':
            add_tweet(status)
        else:
            pass

        # if 'cold' in status.text.lower():
        #     print('cold tweet')

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream

sapi = tweepy.streaming.Stream(auth, CustomStreamListener())
sapi.filter(locations=location)
