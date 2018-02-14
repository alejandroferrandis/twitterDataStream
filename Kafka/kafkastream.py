from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from comun.util import *
from comun.listeners import *
from kafka import SimpleProducer, KafkaClient
import json
import codecs


class StdOutListener(StreamListener):
    def on_data(self, data):
    	decoded = json.loads(data)
        print(decoded['text'])
        producer.send_messages("<>", data.encode('utf-8'))
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
SPAIN_GEOBOX = [-9.38,36.05,3.35,43.75]
stream.filter(languages=["es"], locations=SPAIN_GEOBOX)
