from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from pymongo import MongoClient

MONGO_HOST = 'mongodb://alexander:alexander123@localhost:27017/twitter' 

consumer_key = 'your key'
consumer_secret = 'your secret'
access_token = 'your token'
access_token_secret = 'your token secret'

# WORDS = ['#data', '#science', '#twitter']
	
class StdOutListener(StreamListener):

    def on_connect(self):
        print("You are now connect to the streaming API")
    
    def on_error(self, status_code):
        print("An error has occured: " + repr(status_code))
        
    def on_data(self,data):
        try:
            client = MongoClient(MONGO_HOST)
            db = client.twitter
            datajson = json.loads(data)
            print(datajson['text'])
            tweet = db.twitter_search.insert(datajson)
        except Exception as e:
            print(e)
    
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

l = StdOutListener()
stream = Stream(auth, l)

# print("tracking: " + str(WORDS))
# streamer.filter(track=WORDS)

print("Tracking tweets in spanish: ")

SPAIN_GEOBOX = [-9.38,36.05,3.35,43.75]
stream.filter(languages=["es"], locations=SPAIN_GEOBOX) 


