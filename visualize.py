import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.mllib.linalg import Vectors
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
import matplotlib.pyplot as plt
import pandas as pd

import statistics
import chess.pgn
import chess

def plotDF(pandaDF, xVar, yVar, centroid1, centroid2):
	pandaDF.plot(kind='scatter', x=xVar, y=yVar, color='blue')
	plt.plot(centroid1[0], centroid1[1], 'ro')
	plt.annotate("Centroid 1", (centroid1[0], centroid1[1]))
	plt.plot(centroid2[0], centroid2[1], 'ro')
	plt.annotate("Centroid 2", (centroid2[0], centroid2[1]))

	plt.savefig("{}VS{}".format(xVar, yVar))

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession \
    .builder \
    .appName("ChessKMeans") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/chess_data_testing.games?readPreference=primaryPreferred") \
    .getOrCreate()

model = KMeansModel.load("KMeansModel_final_both_norm")

df = spark.read.parquet("sample.parquet")

# Combine all normalized columns into one "features" column
assembler = VectorAssembler(inputCols=["w_attack_norm", "w_defend_norm", "b_attack_norm", "b_defend_norm", "evals_norm"], outputCol="features")

testing = assembler.transform(df)

transformed = model.transform(testing).select('w_attack_norm', 'w_defend_norm', 'b_attack_norm', 'b_defend_norm', 'evals_norm', 'prediction')
rows = transformed.collect()


df_predictions = sqlContext.createDataFrame(rows)
df_predictions.show()

# pdDF = df.toPandas()

# centroids = model.clusterCenters()

# plotDF(pdDF, 'w_attack_norm', 'w_defend_norm', (centroids[0][0], centroids[0][1]), (centroids[1][0], centroids[1][1]))
# plotDF(pdDF, 'w_attack_norm', 'w_defend_norm', (centroids[0][0], centroids[0][1]), (centroids[1][0], centroids[1][1]))

# pdDF.plot(kind='scatter',x='w_attack_norm',y='w_defend_norm',color='blue')
# plt.plot(centroids[0][0], centroids[0][1], 'ro')
# plt.annotate("Centroid 1", (centroids[0][0], centroids[0][1]))
# plt.plot(centroids[1][0], centroids[1][1], 'ro')
# plt.annotate("Centroid 2", (centroids[1][0], centroids[1][1]))
# plt.savefig("w_attackVSw_defend.png")


# assembler = VectorAssembler(inputCols=["w_attack_norm", "w_defend_norm", "b_attack_norm", "b_defend_norm", "evals_norm"], outputCol="features")

# testing = assembler.transform(normalizedDF)

# transformed = model.transform(testing).select('w_attack_norm', 'w_defend_norm', 'b_attack_norm', 'b_defend_norm', 'evals_norm', 'prediction')
# rows = transformed.collect()

# df_predictions = sqlContext.createDataFrame(rows)
# df_predictions.show()

#