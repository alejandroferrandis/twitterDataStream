# twitterDataStream
Streaming data from twittter to different platforms

Este proyecto muestra el proceso de creación de una arquitectura simple para el procesamiento en tiempo real y por lotes de un programa de “streaming” de tweets, el cual estará corriendo en una maquina virtual con tecnologías open-source del ecosistema Big Data.
Este artículo ha sido redactado por motivos académicos y su propósito no es otro que el de mostrar una solución ejemplificada de el flujo de datos mediante el uso de diferentes herramientas, desde la captura hasta la transformación de los datos, no siendo en ningún caso una solución óptima.
La idea general es la generación de un sistema centralizado para la distribución de mensajes desde diferentes fuentes de datos a múltiples clientes, capaces de consumirlos.

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
