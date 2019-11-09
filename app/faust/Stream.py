import tweepy
import sys
import json
import pandas as pd
import csv
import re #regular expression
from textblob import TextBlob
import string
import preprocessor as p

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

                single_tweet_df = pd.DataFrame([new_entry], columns=COLS)
                df = df.append(single_tweet_df, ignore_index=True)
                # new_file = self.file
                
                with open('%s_tweets.csv' % condition_topic, 'w') as f:
                    writer = csv.writer(f)
                    writer.writerow(COLS)
                    writer.writerows( df.to_csv(f, mode='a', columns=COLS, index=False, encoding="utf-8"))
                   

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
############################
# if __name__ == '__main__':
#     obesity_tweets = "/Users/connorsmith/documents/consensusnetworks_projects/THDashboard/app/web/data/obesity_tweets.csv"
#     diabetes_tweets = "/Users/connorsmith/documents/consensusnetworks_projects/THDashboard/app/web/data/diabetes_tweets.csv"
#     epilepsy_tweets = "data/telemedicine_data_extraction/epilepsy_data.csv"
#     heart_stroke_tweets = "data/telemedicine_data_extraction/heart_stroke_tweets_data.csv"
#     files = [obesity_tweets, diabetes_tweets]

#     for f in files:
#         file = f
#         StreamListener = StreamListener() #Turns Stream Listener Class On
#         StreamListener.field_load(file)
#         print('Streamer On, Ready to Analyze Some Tweets!' + str(file))
        
#         try:
#             print('Waiting For Tweets...')
#             api = getTwitterCredentials(TWITTER_KEY, TWITTER_SECRET, TWITTER_APP_KEY, TWITTER_APP_SECRET)
#             stream = tweepy.Stream(auth = api.auth, listener=StreamListener, aync=True)
#             stream.filter(track = ['obesity', 'diabetes']) 
            
#         except Exception as ex:
#             print ("[STREAM] Stream stopped! Reconnecting to twitter stream")
#             print (ex)
#             stream.filter(track = ['obesity', 'diabetes'])

#         except KeyboardInterrupt:
#             print('Program Exited Gracefully')
#             exit(1)