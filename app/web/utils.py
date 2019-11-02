import tweepy
import sys
import json
import pandas as pd
import csv
import re #regular expression
from textblob import TextBlob
import string
import preprocessor as p
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize




#### Emoji Definitions for Filtering ############################################

#HappyEmoticons
emoticons_happy = set([
    ':-)', ':)', ';)', ':o)', ':]', ':3', ':c)', ':>', '=]', '8)', '=)', ':}',
    ':^)', ':-D', ':D', '8-D', '8D', 'x-D', 'xD', 'X-D', 'XD', '=-D', '=D',
    '=-3', '=3', ':-))', ":'-)", ":')", ':*', ':^*', '>:P', ':-P', ':P', 'X-P',
    'x-p', 'xp', 'XP', ':-p', ':p', '=p', ':-b', ':b', '>:)', '>;)', '>:-)',
    '<3'
    ])

# Sad Emoticons
emoticons_sad = set([
    ':L', ':-/', '>:/', ':S', '>:[', ':@', ':-(', ':[', ':-||', '=L', ':<',
    ':-[', ':-<', '=\\', '=/', '>:(', ':(', '>.<', ":'-(", ":'(", ':\\', ':-c',
    ':c', ':{', '>:\\', ';('
    ])

#combine sad and happy emoticons
emoticons = emoticons_happy.union(emoticons_sad)

#Emoji patterns
emoji_pattern = re.compile("["
         u"\U0001F600-\U0001F64F"  # emoticons
         u"\U0001F300-\U0001F5FF"  # symbols & pictographs
         u"\U0001F680-\U0001F6FF"  # transport & map symbols
         u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
         u"\U00002702-\U000027B0"
         u"\U000024C2-\U0001F251"
         "]+", flags=re.UNICODE)
################################################################################

### Column Definition of the CSV File ##########################################

#columns of the csv file
COLS = ['id', 'created_at', 'source', 'original_text','clean_text', 'sentiment',
'polarity','subjectivity', 'lang','favorite_count', 'retweet_count', 
'original_author',   'possibly_sensitive', 'hashtags','user_mentions', 'place', 
'place_coord_boundaries']

################################################################################

### Twitter API Authorization Fxn ##############################################
def getTwitterCredentials(TWITTER_KEY, TWITTER_SECRET, TWITTER_APP_KEY, 
                            TWITTER_APP_SECRET):
    #Gathers Twitter Keys
    auth = tweepy.OAuthHandler(TWITTER_KEY, TWITTER_SECRET) 
    #Gathers Twitter APP Keys
    auth.set_access_token(TWITTER_APP_KEY, TWITTER_APP_SECRET) 
    api = tweepy.API(auth)

    return api
################################################################################

### Fxn to Preprocess Tweets Prior to Analysis #################################
def clean_tweets(tweet):
    stop_words = set(stopwords.words('english'))
    word_tokens = word_tokenize(tweet)
#after tweepy preprocessing the colon symbol left remain after      
    #removing mentions
    tweet = re.sub(r':', '', tweet)
    tweet = re.sub(r'‚Ä¶', '', tweet)
#replace consecutive non-ASCII characters with a space
    tweet = re.sub(r'[^\x00-\x7F]+',' ', tweet)
#remove emojis from tweet
    tweet = emoji_pattern.sub(r'', tweet)
#filter using NLTK library append it to a string
    filtered_tweet = [w for w in word_tokens]
    filtered_tweet = []
#looping through conditions
    for w in word_tokens:
        #check tokens against stop words , emoticons and punctuations
        if w not in emoticons and w not in string.punctuation:
            filtered_tweet.append(w)
    return ' '.join(filtered_tweet)
    # print(word_tokens)
    # print(filtered_sentence)
##################################################################################
