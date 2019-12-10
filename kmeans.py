## NOTE: This isn't working... Moved to scala instead because documentation was better

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

from numpy import array
from math import sqrt

from pyspark.mllib.clustering import KMeans, KMeansModel

sc = SparkContext()
spark = SparkSession \
    .builder \
    .appName("ChessKMeans") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/chess_data.games_collection?readPreference=primaryPreferred") \
    .getOrCreate()

df = spark.read.format("mongo").load()

df.printSchema()


# Build the model (cluster the data)
# clusters = KMeans.train(parsedData, 2, maxIterations=10, initializationMode="random")

# # Evaluate clustering by computing Within Set Sum of Squared Errors
# def error(point):
#     center = clusters.centers[clusters.predict(point)]
#     return sqrt(sum([x**2 for x in (point - center)]))

# WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
# print("Within Set Sum of Squared Error = " + str(WSSSE))

# # Save and load model
# clusters.save(sc, "target/org/apache/spark/ChessKMeans/KMeansModel")
# sameModel = KMeansModel.load(sc, "target/org/apache/spark/ChessKMeans/KMeansModel")

# ~/spark/spark-2.4.4-bin-hadoop2.7/bin/pyspark --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/chess_data.games_collection?readPreference=primaryPreferred" --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1
"/home/ryan/spark/spark-2.4.4-bin-hadoop2.7"