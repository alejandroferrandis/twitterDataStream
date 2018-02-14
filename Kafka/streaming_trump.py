import tweepy

from kafka import *
from comun.util import *
from comun.listeners2 import *

if __name__ == '__main__':

    print("===== My Application =====")

    auth = get_auth()  
    api = tweepy.API(auth) 

    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic_name = 'trump'

    myStreamListener = MyStreamListener2()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

    SPAIN_GEOBOX = [-9.38,36.05,3.35,43.75]
    myStream.filter(languages=["es"], locations=SPAIN_GEOBOX)

    print("c'est fini!")