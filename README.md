# twitterDataStream
Streaming data from twittter to different platforms

Este proyecto muestra el proceso de creación de una arquitectura simple para el procesamiento en tiempo real y por lotes de un programa de “streaming” de tweets, el cual estará corriendo en una maquina virtual con tecnologías open-source del ecosistema Big Data.
Este artículo ha sido redactado por motivos académicos y su propósito no es otro que el de mostrar una solución ejemplificada de el flujo de datos mediante el uso de diferentes herramientas, desde la captura hasta la transformación de los datos, no siendo en ningún caso una solución óptima.
La idea general es la generación de un sistema centralizado para la distribución de mensajes desde diferentes fuentes de datos a múltiples clientes, capaces de consumirlos.

En este proyecto nos centraremos en la adquisición de datos a través de 3 sistemas diferentes:

   MongoDB
   Kafka
   SQL

Cada sistema será independiente el uno del otro, siendo la fuente de información (twitter) la misma para los 3.

Arquitectura de nuestro sistema:

![alt text](https://github.com/alejandroferrandis/twitterDataStream/blob/master/Images/imagen6.png)

## 1.	Pre-requisitos y configuración

Nuestro sistema estará montado en una máquina virtual Cloudera. Este sistema incluye ya de forma predeterminada las herramientas básicas de Big data, nosotros únicamente deberemos retocarla un poco para ajustarla a nuestras necesidades:

•	Instalaremos minicoda para actualizar la versión de python y añadiremos los paquetes necesarios

```bash
cd
wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
chmod a+x Miniconda2-latest-Linux-x86_64.sh
./Miniconda2-latest-Linux-x86_64.sh

source $HOME/.bashrc
python --version

rm Miniconda2-latest-Linux-x86_64*

pip install tweepy
pip install PrettyTable
pip install pprintpp
pip install pandas

```

•	Instalaremos MongoDB para el almacenamiento de los datos. Nos descargamos el repositorio que lo incluye y lo agregamos a nuestro sistema

```bash
cd
git clone https://github.com/dvillaj/Hadoop.git

sudo cp $HOME/Hadoop/config/mongodb-org-3.6.repo /etc/yum.repos.d/mongodb-org-3.6.repo

sudo yum install -y mongodb-org

sudo sed -i 's/bindIp: 127.0.0.1/bindIp: 0.0.0.0/' /etc/mongod.conf
sudo service mongod restart

pip install pymongo
```

•	Instalaremos Kafka para la realización del streaming

```bash
cd
wget http://apache.rediris.es/kafka/1.0.0/kafka_2.11-1.0.0.tgz
tar -xzf kafka_2.11-1.0.0.tgz

echo 'export KAFKA_HOME=$HOME/kafka_2.11-1.0.0' >> $HOME/.bashrc
echo 'export PATH=$KAFKA_HOME/bin:$PATH' >> $HOME/.bashrc
source $HOME/.bashrc
```

## 2. Twitter stream a MongoDB

El siguiente paso será establecer nuestro stream desde twitter a la base de datos que deseemos. Para ello primero deberemos crear la base de datos.

Entramos en MongoDB

```bash
$ mongo
```

Creamos nuestra base de datos y un usuario con permisos de administrador 

```bash
> use <nombre BBDD>
> db.createUser(
   {
     user: "<usuario>",
     pwd: "<contraseña>",
     roles: [ "readWrite", "dbAdmin" ]
   }
 )

```

Descargamos este repositorio

```bash
cd
$ git clone .....

```

Añadiremos nuestras claves de twitter dentro del archivo "secret.py" Luego introduciremos el usuario, contraseña y nombre de la base de datos dentro el archivo mongostream.py

Si todo ha salido bien, ejecutando el siquiente comando

```bash

$ mongo

```

```bash

> db.<nombre BBDD>.find()

```
Debería mostrarnos el contenido que se ha ido almacenando

![alt text](https://github.com/alejandroferrandis/twitterDataStream/blob/master/Images/imagen1.png)

Estas bases de datos podrán ser analizadas posteriormente con dbKoda. DbKoda es una herramienta especializada para el análisis de bases de datos en MongoDB. Se trata de una herramienta muy potente y que permite realizar de manera sencilla la conexion con nuestro servidor.

![alt text](https://github.com/alejandroferrandis/twitterDataStream/blob/master/Images/imagen2.png)

## Variaciones

Para realizar de manera mas cómoda la recolección de datos podemos usar otra herramienta que se dedique específicamente a ello. DigitalOcean ofrece una plataforma barata para la creación de una maquina virtual, la cual podemos configurar para que ejecute este mismo programa.

![alt text](https://github.com/alejandroferrandis/twitterDataStream/blob/master/Images/imagen3.png)

Añadiendo acceso ssh podremos entrar desde nuestra maquina de manera sencilla

![alt text](https://github.com/alejandroferrandis/twitterDataStream/blob/master/Images/imagen4.png)

## 3. Kafka stream

Kafka es otra herramienta que se basa en el sistema de subscriptor-cliente para la publicacion de los datos en diferentes plataformas.
Dado que utiliza los llamados 'topics', podemos de ser necesario, ir añadiendo mas tópicos, haciendo de ella una herramienta con alta escalabilidad.

Lo primero que deberemos hacer será descargar Kafka en nuestro sistema:
```
$ cd
$ wget http://apache.rediris.es/kafka/1.0.0/kafka_2.11-1.0.0.tgz
$ tar -xzf kafka_2.11-1.0.0.tgz

$ echo 'export KAFKA_HOME=$HOME/kafka_2.11-1.0.0' >> $HOME/.bashrc
$ echo 'export PATH=$KAFKA_HOME/bin:$PATH' >> $HOME/.bashrc
$ source $HOME/.bashrc

```

Una vez realizado, arrancamos nuestro servidor 

```
$ bin/zookeeper-server-start.sh config/zookeeper.properties
$ bin/kafka-server-start.sh config/server.properties

```

Creamos nuestro tópico
```
$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic <nombre tópico>
```

Si queremos ver el los datos que se van añadiendo lo podemos hacer con:
```
$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <nombre tópico> --from-beginning 
```

Los datos que vamos almacenando pueden ser introducidos en HDFS para su posterior análisis usando flume. Para ello deberemos crear un archivo '<nombre_archivo>.conf' con el siguiente contenido

```
flume1.sources = kafka-source-1
flume1.channels = hdfs-channel-1
flume1.sinks = hdfs-sink-1
flume1.sources.kafka-source-1.type = org.apache.flume.source.kafka.KafkaSource
flume1.sources.kafka-source-1.zookeeperConnect = localhost:2181
flume1.sources.kafka-source-1.topic =<nombre tópico>
flume1.sources.kafka-source-1.batchSize = 100
flume1.sources.kafka-source-1.channels = hdfs-channel-1

flume1.channels.hdfs-channel-1.type = memory
flume1.sinks.hdfs-sink-1.channel = hdfs-channel-1
flume1.sinks.hdfs-sink-1.type = hdfs

flume1.sinks.hdfs-sink-1.hdfs.fileType = DataStream
flume1.sinks.hdfs-sink-1.hdfs.filePrefix = test-events
flume1.sinks.hdfs-sink-1.hdfs.useLocalTimeStamp = true
flume1.sinks.hdfs-sink-1.hdfs.path = /tmp/kafka/%{topic}/%y-%m-%d
flume1.sinks.hdfs-sink-1.hdfs.rollCount=100
flume1.sinks.hdfs-sink-1.hdfs.rollSize=0
flume1.channels.hdfs-channel-1.capacity = 10000
flume1.channels.hdfs-channel-1.transactionCapacity = 1000

```

Una vez creado, arrancamos nuestro servicio:

```
flume-ng agent -n flume1 -c conf -f <nombre_archivo>.conf -    Dflume.root.logger=INFO,console
```

Los datos serán guardados dentro de la carpeta 
```
/tmp/kafka/%{topic}/%y-%m-%d
```

![alt text](https://github.com/alejandroferrandis/twitterDataStream/blob/master/Images/imagen5.png)

Pasando el archivo posteriormente a hive podríamos realizar el ánalisis del mismo o usar Impala y un conector ODBC para analizar los parametros necesarios en herramientas como Spotfire o Tableau.


## 4. Twitter stream a SQL

SQL es un lenguaje basado, a diferencia de los otros dos, en un sistema de consulta relacional. Las consultas y adquisiciones de datos que se realizan en el mismo estan siempre estructuradas, permitiendo así una alta funcionalidad de los mismos. Sin embargo su velocidad de procesamiento respecto a los otros dos es bastante lenta y el volumen de datos que puede tratar para la realizacion de análisis es inferior.

Lo primero que deberemos realizar será la creación de nuestra base de datos. Para ello entraremos en la plataforma mysql:

```
$ mysql -uroot -pcloudera
```

Le indicamos el nombre de nuestra BBDD deseada

```
mysql> CREATE DATABASE <nombre_BBDD>
```
Una vez creada, mediante el comando 'quit' saldremos de la plataforma.

```
mysql> quit;
```

Para la adquisición de los datos usaremos una librería llamada mysql.connector. Buscaremos primero el modulo:

```
$ pip search mysql-connector | grep --color mysql-connector-python


mysql-connector-python-rf (version)        - MySQL driver written in Python
mysql-connector-python (version)           - MySQL driver written in Python
```

Instalaremos mysql-connector-python-rf

```
$ pip install mysql-connector-python-rf
```

Una vez tengamos ya toda nuestra plataforma preparada, podremos empezar con la toma de datos. Para la adquisición de estos, deberemos valorar primero cuales son los que nos interesa recoger, puesto que en SQL los que vamos a almacenar son aquellos que nosotros le solicitemos.

En nuestro caso serán los siguientes:

ID
NOMBRE
NOMBRE_USUARIO
TEXTO
DESCRIPCION
SEGUIDORES
AMIGOS
LOCALIDAD
GEOBOX
CANTIDAD_FAVORITOS
RETWEETS
FAVORITOS

Creamos el siguiente archivo .py para producción de las tablas en nuestra base de datos.

```
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime  
from datetime import timedelta  

def date_today():

    dateAndTime = datetime.now() + timedelta(hours = 9  )

    race = '{0}'.format(dateAndTime.day)

    if dateAndTime.month == 1:

        race += 'Enero'

    if dateAndTime.month == 2:
    
        race += 'Febrero'

    if dateAndTime.month == 3:
    
        race += 'Marzo'

    if dateAndTime.month == 4:
    
        race += 'Abril'
    
    if dateAndTime.month == 5:
    
        race += 'Mayo'
      
    if dateAndTime.month == 6:
    
        race += 'Junio'
    
    if dateAndTime.month == 7:
    
        race += 'Julio'
    
    if dateAndTime.month == 8:
    
        race += 'Agosto'
    
    if dateAndTime.month == 9:
    
        race += 'Septiembre'
    
    if dateAndTime.month == 10:
    
        race += 'Octubre'
    
    if dateAndTime.month == 11:
    
        race += 'Noviembre'
    
    if dateAndTime.month == 12:

        race += 'Diciembre'

    race += '{0}'.format(dateAndTime.year)

    print("El nombre de la tabla es: " + race)

    return race

#connect to db
cnx = mysql.connector.connect(user='root',password='cloudera', database='<nombre_BBDD>')

#setup cursor
cursor = cnx.cursor()

entrada1 = date_today()


table = (
    """CREATE TABLE `%s` (ID VARCHAR(50) DEFAULT NULL, NOMBRE VARCHAR(30) DEFAULT NULL, NOMBRE_USUARIO VARCHAR(30) DEFAULT NULL, 
       TEXTO VARCHAR(160) DEFAULT NULL, DESCRIPCION VARCHAR(160) DEFAULT NULL, SEGUIDORES VARCHAR(8) DEFAULT NULL, 
       AMIGOS VARCHAR(5) DEFAULT NULL, LOCALIDAD VARCHAR(50) DEFAULT NULL, GEOBOX VARCHAR(120) DEFAULT NULL, 
       CANTIDAD_FAVORITOS VARCHAR(8) DEFAULT NULL, RETWEETS VARCHAR(8) DEFAULT NULL, FAVORITOS VARCHAR(8) DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1""" % (entrada1))




try:
    print("Creating table.... ")
    cursor.execute(table)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
        print("Error, already exists.")
    else:
        print(err.msg)
else:
    print("OK")

cursor.close()
cnx.close()
```

La función date_today() se encarga de nombrar nuestra tabla de forma automatica. Dado que se trata de la adquisición de datos mediante streaming a una tabla SQL y desconocemos la rapidez con la que nos van a llegar los mismos, he decidido, que todas las entradas de la tabla sean VARCHAR.

Lo siguiente será adquirir nuestros datos. El archivo 'upload_tweets_sql.py' se encarga de ello. Al ejecutarlo nos irá apareciendo en pantalla el texto de cada uno de los tweets que sea importado en nuestro sistema.

Tras tener nuestra primera tabla montada, podremos importarla mediante sqoop para que sea analizada posteriormente con Hive, Impala o Tableau. El siguiente comando se ocupa de la importación de estos. 

Nota: Al no tener primary key necesitamos o bien un '--split by' o '-m 1' para la importación de los datos

```
sqoop import \
--connect jdbc:mysql://localhost/<nombre_BBDD> \
--table <nombre_tabla> --fields-terminated-by '\t' \
--username root \
--password cloudera \
--target-dir /user/root/user_data \
-m 1
```

Comprobamos que la importación se ha realizado de forma correcta.

![alt text](https://github.com/alejandroferrandis/twitterDataStream/blob/master/Images/imagen7.png)

Si la importación se ha realizado sin problemas podremos pasar a importar nuestro archivo a hive. Entraremos en la plataforma mediante el siguiente comando:

```
$ hive
```

Y crearemos la tabla con los registros de nuestra tabla SQL

```
hive > CREATE EXTERNAL TABLE tweets_prueba
(ID STRING, NOMBRE STRING, NOMBRE_USUARIO STRING,
TEXTO STRING, DESCRIPCION STRING, SEGUIDORES STRING,
AMIGOS STRING, LOCALIDAD STRING, GEOBOX STRING,
CANTIDAD_FAVORITOS STRING, RETWEETS STRING, FAVORITOS STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/root/user_data';
```

![alt text](https://github.com/alejandroferrandis/twitterDataStream/blob/master/Images/imagen8.png)

Ya por ultimo solo nos quedaría acceder a la base de datos a traves de Tableau mediante el conector ODBC Impala, siendo el servidor 'localhost', el puerto '21050' y la base de datos 'IMPALA'.

![alt text](https://github.com/alejandroferrandis/twitterDataStream/blob/master/Images/imagen9.png)

Como se puede observar, existen fallos en los registros. Esto se puede deber en parte a que nuestra conexion en streaming no es capaz de almacenar tan rapido los tweets como le van llegando. La solción a este problema sería crear un archivo JSON temporal que almacene dichos datos y estos sean luego transferidos a SQL sin problemas.


