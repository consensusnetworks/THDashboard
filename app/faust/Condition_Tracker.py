import tweepy
import sys
import json
import faust
import pandas as pd
import csv
import re #regular expression
from textblob import TextBlob
import string
import preprocessor as p
import psycopg2

from credentials import TWITTER_KEY, TWITTER_SECRET, TWITTER_APP_KEY, TWITTER_APP_SECRET
from utils import clean_tweets, COLS, emoticons, emoji_pattern, getTwitterCredentials
from Stream import StreamListener

app = faust.App('Condition_Tracker',  broker='kafka://localhost:9092')
print(TWITTER_KEY, TWITTER_SECRET, TWITTER_APP_KEY, TWITTER_APP_SECRET)

source_topic = app.topic('Conditions', value_serializer='json')

@app.agent(source_topic)
async def getAccounts(source_topic):
    async for topic in source_topic:
        print(topic)
        condition_topic = app.topic(str(topic), value_serializer='json')
        condition_topic.stream()
        print(condition_topic)
        print('New Stream Made')

        Stream_Listener = StreamListener() #Turns Stream Listener Class On
        Stream_Listener.field_load(condition_topic)

        try:
            api = getTwitterCredentials(TWITTER_KEY, TWITTER_SECRET, TWITTER_APP_KEY, TWITTER_APP_SECRET) #authorize api credentials
            stream = tweepy.Stream(auth = api.auth, listener=Stream_Listener, aync=True) #create a stream for the account
            stream.filter(track = [str(condition_topic)], is_async=True) #listens to twitter account and triggers for only the account's tweets
        
        except Exception as ex: #error handling to restart streamer in the event of it stopping for things like Rate Limit Error
            print ("[STREAM] Stream stopped! Reconnecting to twitter stream")
            print (ex)
            stream.filter(track = [str(condition_topic)])

       