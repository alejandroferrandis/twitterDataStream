import mysql.connector
from mysql.connector import errorcode
from datetime import datetime  
from datetime import timedelta  
import json
import codecs
import tweepy
from comun.util import *


class MyStreamListener(tweepy.StreamListener):

    def on_data(self, data):
        try:
            decoded = json.loads(data)
            print(decoded['text'])

            geoloc = ''.join(str(v) for v in decoded['place']['bounding_box']['coordinates'])
      
            add_registry = ("""INSERT INTO <nombre_tabla> """
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

            # Insert new employee

            cursor.execute(add_registry, (decoded['id'], decoded['user']['name'], decoded['user']['screen_name'], decoded['text'], decoded['user']['description'], decoded['user']['followers_count'], decoded['user']['friends_count'], decoded['place']['name'], geoloc, decoded['user']['favourites_count'], decoded['retweet_count'], decoded['favorite_count']))

            geoloc = None
            # Make sure data is committed to the database
            cnx.commit()

        except Exception as e:
            print("ERROR: {}".format(e))
        finally:
            return True  # Keep listening

    def on_error(self, status): 
        print("Error %i" % status)

#connect to db
cnx = mysql.connector.connect(user='root',password='cloudera', database='<nombre_BBDD>')

#setup cursor
cursor = cnx.cursor()

if __name__ == '__main__':
    print("===== My Application =====")

    auth = get_auth()  
    api = tweepy.API(auth) 

    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

    # LOCATIONS. Use http://boundingbox.klokantech.com/ for boundingboxes
    SPAIN_GEOBOX = [-9.38,36.05,3.35,43.75]
    myStream.filter(languages=["es"], locations=SPAIN_GEOBOX)

    print("c'est fini!")

cursor.close()
cnx.close()
