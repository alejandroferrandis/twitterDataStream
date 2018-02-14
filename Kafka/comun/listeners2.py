import json
import codecs
import tweepy
import os

class MyStreamListener2(tweepy.StreamListener):

    def on_data(self, data):
        try:
            decoded = json.loads(data)
            print(decoded['text'])

            if not os.path.exists("data"):
                os.makedirs("data")

        except Exception as e:
            print("ERROR: {}".format(e))
        finally:
            return True  # Keep listening

    def on_error(self, status): 
        print("Error %i" % status) 
