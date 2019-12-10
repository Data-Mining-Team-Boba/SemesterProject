# SemesterProject

## Virtual environment
* Creating: `virtualenv <path to env>/pyspark-env`
* Using: `source <path to env>/pyspark-env/bin/activate`
* Install packages: `pip3 install --requirement requirements.txt`

## Copy to Compute Engine:
```
gcloud compute scp --project="chess-attack-defend-power" --zone="us-central1-a" parser.py mongo-and-api:~/
```

## Spark installation (local)
* NOTE: You need `java` and `scala` installed
* Download spark from: http://spark.apache.org/downloads.html
* Untar it
* Install pyspark: `pip3 install pyspark`
* Add these two lines: `vim ~/.zshrc` (replace this with ~/.bash_rc if you are using bash)
	```
	export SPARK_HOME="<Absolute path to untarred spark>/spark-x.x.x-bin-hadoopx.x"
	export PATH="$SPARK_HOME/bin:$PATH"
	```

## Mongo Spark Connector Setup
* Go into this directory: `<Absolute path to untarred spark>/jars`
* Download Mongo Connector jar: `wget http://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.11/2.4.1/mongo-spark-connector_2.11-2.4.1.jar`
	* NOTE: This version needs to match your spark version (Ex. 2.4.x)
* Get all these jars:
```
wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver/3.8.1/mongodb-driver-3.8.1.jar
wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/3.8.1/mongodb-driver-core-3.8.1.jar
wget https://repo1.maven.org/maven2/org/mongodb/bson/3.11.2/bson-3.11.2.jar
```


## Running Spark job
```
spark-submit spark-kmeans_2.11-1.0.jar
```

<!-- Scala chess: https://github.com/ornicar/scalachess -->