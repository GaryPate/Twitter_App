import tweepy
import time
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
from http.client import IncompleteRead
import csv


consumer_key = None
consumer_secret = None
access_token = None
access_secret = None
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth, timeout=90)


with open('result.csv', 'a') as csvdump:
    wTweet = csv.writer(csvdump, delimiter=';')

class Listener(StreamListener):
    def on_status(self, status):
        if status.lang == 'en':
            print(status.text)
            with open("UTWriter.txt", "a", encoding='utf-8') as writer:
                writer.write(status.text + "\n")

#def on_error(self, status):
#    print(status)
#    return True

while True:
    try:
        twitter_stream = Stream(auth, Listener())
        twitter_stream.sample()

    except IncompleteRead:
        pass

    except KeyboardInterrupt:
        twitter_stream.disconnect()
        break

    #finally:  # Is this ok, may result in rate limiting?
    #    pass

def record_data(input):
    for line in input:
        with open("record_data", "w") as log:
            log.write(line)
