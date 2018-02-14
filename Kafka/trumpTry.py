from __future__ import print_function
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
import json
import codecs

# Iniciamos kafka con los siguientes comandos:
# bin/zookeeper-server-start.sh config/zookeeper.properties
# bin/kafka-server-start.sh config/server.properties

# Para empezar a observar el streaming usamos el siguiente comando:
# El topico debe de coincidir!!!!!
# bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trump --from-beginning

consumer_key = 'fIiBGbdnptl6xvNWDItZ7yodh'
consumer_secret = '1CQOgjh9TE5KMv0rGjMf6DPXeDRprsEdr4KqeImsf3GCrH7v0j'
access_token = '952980946978070535-SEpyDDiuGL4fiYyV5ZwPAd5CqzSGlhk'
access_token_secret = 'njtsvXJmDGVnmhxQ3e49RWGggJ6Le4Piv9uMCQw6hTfT0'

class StdOutListener(StreamListener):
    def on_data(self, data):
    	decoded = json.loads(data)
        print(decoded['text'])
        producer.send_messages("trump", data.encode('utf-8'))
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

