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

            # add_employee = ("""INSERT INTO 15Febrero2018 """
            #    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

            add_employee = ("""INSERT INTO 15Febrero2018 """
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

            # Insert new employee

            # cursor.execute(add_employee, (decoded['id'], decoded['user']['name'], decoded['user']['screen_name'], decoded['text'], decoded['user']['description'], decoded['user']['followers_count'], decoded['user']['friends_count'], decoded['place']['name'], decoded['place']['bounding_box']['coordinates'], decoded['user']['favourites_count'], decoded['retweet_count'], decoded['favorite_count']))
            
            cursor.execute(add_employee, (decoded['id'], decoded['user']['name'], decoded['user']['screen_name'], decoded['text'], decoded['user']['description'], decoded['user']['followers_count'], decoded['user']['friends_count'], decoded['place']['name'], geoloc, decoded['user']['favourites_count'], decoded['retweet_count'], decoded['favorite_count']))

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
cnx = mysql.connector.connect(user='root',password='cloudera', database='tweets_esp')

#setup cursor
cursor = cnx.cursor()

if __name__ == '__main__':
    print("===== My Application =====")

    auth = get_auth()  
    api = tweepy.API(auth) 

    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

    #print(">> Listening to tweets about #python:")
    #myStream.filter(track=['python', 'NoSQL'])

    # LOCATIONS. Use http://boundingbox.klokantech.com/ for boundingboxes
    SPAIN_GEOBOX = [-9.38,36.05,3.35,43.75]
    myStream.filter(languages=["es"], locations=SPAIN_GEOBOX)

    print("c'est fini!")

cursor.close()
cnx.close()