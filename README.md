# SemesterProject

## Virtual Environment
* Creating: `virtualenv <path to env>/pyspark-env`
* Using: `source <path to env>/pyspark-env/bin/activate`
* Install packages: `pip3 install --requirement requirements.txt`

## Spark Installation (local)
* NOTE: You need `java` and `scala` installed
* Download spark from: http://spark.apache.org/downloads.html
* Untar it
* Install pyspark: `pip3 install pyspark`
* Add these two lines: `vim ~/.zshrc` (replace this with ~/.bash_rc if you are using bash)
	```
	export SPARK_HOME="<Absolute path to untarred spark>/spark-2.4.4-bin-hadoop2.7"
	export PATH="$SPARK_HOME/bin:$PATH"
	```

## Mongo Spark Connector Setup
* Go into this directory: `<Absolute path to untarred spark>/jars`
* Download Mongo Connector jar: `wget http://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.11/2.4.1/mongo-spark-connector_2.11-2.4.1.jar`
	* NOTE: This version needs to match your spark version (Ex. 2.4.x)
* Get all these jars (NOTE: I believe this is all of them, but it could be missing some):
```
wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver/3.8.1/mongodb-driver-3.8.1.jar
wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/3.8.1/mongodb-driver-core-3.8.1.jar
wget https://repo1.maven.org/maven2/org/mongodb/bson/3.11.2/bson-3.11.2.jar
```
* If you are missing a jar look for the `NoClassDef` line and try to install the corresponding jar file

## Preprocessing
NOTE: Make sure you have MongoDB running locally

* Run: `python3 parser.py <pgn file> <optional: starting game number>`

## Attacking and Defending Feature Logic
NOTE: Make sure you have MongoDB running locally

* Run: `python3 model.py`
	* This function was not explicitly used to build a k-means model but the logic is the same
	* This function also contains a SKLearn function

## Building KMeans Model
NOTE: Make sure pyspark and dependencies are installed

* Run: `spark-submit kmeans.py`
	* This will take around ~1 hour to run
	* What this does is it takes all the data from the mongo database, converts the game data into numeric values, normalizes the feature values, then trains a kmeans model
* The model should be saved locally as `KMeansModel` (this is a directory)

## Adding Test Dataset
NOTE: You need csv files with the format (can generate these sequences from https://lichess.org/analysis)
```
FEN,Centipawn evaluation
FEN,Centipawn evaluation
...
```

* Run: `python3 uploadLabelledCSV.py <csv file>`


## Evaluating the model
NOTE: You need to have a model saved and test data uploaded

* Run: `spark-submit evaluate.py`
	* Might need to change the `model-name` in the file if you didn't end up using `KMeansModel` as the name
