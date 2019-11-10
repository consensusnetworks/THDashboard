import tweepy
import sys
import json
import pandas as pd
import csv
import re #regular expression
from textblob import TextBlob
import string
import preprocessor as p
import psycopg2

from credentials import EC_ADDRESS, FCT_ADDRESS, TWITTER_KEY, TWITTER_SECRET, TWITTER_APP_KEY, TWITTER_APP_SECRET
from utils import clean_tweets, COLS, emoticons, emoji_pattern, getTwitterCredentials 

class StreamListener(tweepy.StreamListener):
    def field_load(self, condition_topic):
        self.condition_topic = condition_topic

        print(self.condition_topic)
    def on_status(self, status):

        try:
            new_entry = []
            status = status._json
            print('status', status['lang'])

            condition_topic = self.condition_topic
            df = pd.DataFrame(columns=COLS)
            if status['lang'] == 'en':
                #tweepy preprocessing called for basic preprocessing
                clean_text = p.clean(status['text'])
                print(clean_text)
                #call clean_tweet method for extra preprocessing
                filtered_tweet = clean_tweets(clean_text)
                print(filtered_tweet)
                #pass textBlob method for sentiment calculations
                blob = TextBlob(filtered_tweet)
                print(blob)
                Sentiment = blob.sentiment
                print(Sentiment)

                #seperate polarity and subjectivity in to two variables
                polarity = Sentiment.polarity
                subjectivity = Sentiment.subjectivity

                #new entry append
                new_entry += [status['id'], status['created_at'],
                            status['source'], status['text'],filtered_tweet, Sentiment,polarity,subjectivity, status['lang'],
                            status['favorite_count'], status['retweet_count']]
    
                #to append original author of the tweet
                new_entry.append(status['user']['screen_name'])
    
                try:
                    is_sensitive = status['possibly_sensitive']
                except KeyError:
                    is_sensitive = None
                    new_entry.append(is_sensitive)


                # hashtagas and mentiones are saved using comma separted
                hashtags = ", ".join([hashtag_item['text'] for hashtag_item in status['entities']['hashtags']])
                new_entry.append(hashtags)
                mentions = ", ".join([mention['screen_name'] for mention in status['entities']['user_mentions']])
                new_entry.append(mentions)


                #get location of the tweet if possible
                try:
                    location = status['user']['location']
                except TypeError:
                    location = ''
                new_entry.append(location)
    
                try:
                    coordinates = [coord for loc in status['place']['bounding_box']['coordinates'] for coord in loc]
                except TypeError:
                    coordinates = None
                new_entry.append(coordinates)
                print(new_entry)
                
                con = psycopg2.connect("host='localhost' dbname='conditions' user='connorsmith' password='smith95'")
                sql = "INSERT INTO " + str(condition_topic) + """(id, created_at, source, original_text, clean_text, 
                                                                sentiment, polarity, subjectivity, lang, favorite_count, 
                                                                retweet_count, original_author, possibly_sensitive, hashtags, 
                                                                user_mentions, place) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, 
                                                                %s, %s, %s, %s, %s, %s, %s, %s)"""
                cur = con.cursor()
                cur.execute(sql, (new_entry[0], new_entry[1], new_entry[2], new_entry[3], new_entry[4], new_entry[5], new_entry[6],
                                  new_entry[7], new_entry[8], new_entry[9], new_entry[10], new_entry[11], new_entry[12], new_entry[13], 
                                  new_entry[14], new_entry[15]))
                con.commit()
                cur.close()
                   

        except BaseException as e:
                print("Error on_data %s" % str(e))
                return True
        

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream
        print ("Stream restarted")

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream
        print ("Stream restarted")