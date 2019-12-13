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

# def plotDF(pandaDF, xVar, yVar, centroid1, centroid2, point_color):
# 	pandaDF.plot(kind='scatter', x=xVar, y=yVar, color=point_color)
# 	plt.plot(centroid1[0], centroid1[1], 'ro')
# 	plt.annotate("Centroid 1", (centroid1[0], centroid1[1]))
# 	plt.plot(centroid2[0], centroid2[1], 'ro')
# 	plt.annotate("Centroid 2", (centroid2[0], centroid2[1]))
#
# 	plt.savefig("{}VS{}".format(xVar, yVar))

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

####
pdDF = df_predictions.toPandas()

centroids = model.clusterCenters()

# centroid 1 => prediction = 0 => attack
# centroid 2 => prediction = 1 => defend
# df_test = pdDF.iloc[:10]
# print(df_test.head(10))
# for col in df_test.columns:
#     print(col)
# df_attack = df_test[pdDF["prediction"] == 0]
# print(df_attack.head(10))
# df_defend = df_test[pdDF["prediction"] == 1]
# print(df_defend.head(10))

df_attack = pdDF[pdDF["prediction"] == 0]
df_defend = pdDF[pdDF["prediction"] == 1]

# white_attack_norm versus white_defend_norm
wa = df_attack.plot(kind='scatter', x='w_attack_norm', y='w_defend_norm', color='blue', label='attacking')
wd = df_defend.plot(kind='scatter', x='w_attack_norm', y='w_defend_norm', color='red', label='defending', ax=wa)
plt.plot(centroids[0][0], centroids[0][1], 'bo')
plt.annotate("Centroid 1", (centroids[0][0], centroids[0][1]))
plt.plot(centroids[1][0], centroids[1][1], 'ro')
plt.annotate("Centroid 2", (centroids[1][0], centroids[1][1]))
plt.legend()

plt.savefig("w_attack_normVSw_defend_norm.png")

# white_attack_norm versus black_attack_norm
wa = df_attack.plot(kind='scatter', x='w_attack_norm', y='b_attack_norm', color='blue', label='attacking')
wd = df_defend.plot(kind='scatter', x='w_attack_norm', y='b_attack_norm', color='red', label='defending', ax=wa)
plt.plot(centroids[0][0], centroids[0][2], 'bo')
plt.annotate("Centroid 1", (centroids[0][0], centroids[0][2]))
plt.plot(centroids[1][0], centroids[1][2], 'ro')
plt.annotate("Centroid 2", (centroids[1][0], centroids[1][2]))
plt.legend()

plt.savefig("w_attack_normVSb_attack_norm.png")

# white_attack_norm versus black_defend_norm
wa = df_attack.plot(kind='scatter', x='w_attack_norm', y='b_defend_norm', color='blue', label='attacking')
wd = df_defend.plot(kind='scatter', x='w_attack_norm', y='b_defend_norm', color='red', label='defending', ax=wa)
plt.plot(centroids[0][0], centroids[0][3], 'bo')
plt.annotate("Centroid 1", (centroids[0][0], centroids[0][3]))
plt.plot(centroids[1][0], centroids[1][3], 'ro')
plt.annotate("Centroid 2", (centroids[1][0], centroids[1][3]))
plt.legend()

plt.savefig("w_attack_normVSb_defend_norm.png")

# white_defend_norm versus black_attack_norm
wa = df_attack.plot(kind='scatter', x='w_defend_norm', y='b_attack_norm', color='blue', label='attacking')
wd = df_defend.plot(kind='scatter', x='w_defend_norm', y='b_attack_norm', color='red', label='defending', ax=wa)
plt.plot(centroids[0][1], centroids[0][2], 'bo')
plt.annotate("Centroid 1", (centroids[0][1], centroids[0][2]))
plt.plot(centroids[1][1], centroids[1][2], 'ro')
plt.annotate("Centroid 2", (centroids[1][1], centroids[1][2]))
plt.legend()

plt.savefig("w_defend_normVSb_attack_norm.png")

# white_defend_norm versus black_defend_norm
wa = df_attack.plot(kind='scatter', x='w_defend_norm', y='b_defend_norm', color='blue', label='attacking')
wd = df_defend.plot(kind='scatter', x='w_defend_norm', y='b_defend_norm', color='red', label='defending', ax=wa)
plt.plot(centroids[0][1], centroids[0][3], 'bo')
plt.annotate("Centroid 1", (centroids[0][1], centroids[0][3]))
plt.plot(centroids[1][1], centroids[1][3], 'ro')
plt.annotate("Centroid 2", (centroids[1][1], centroids[1][3]))
plt.legend()

plt.savefig("w_defend_normVSb_defend_norm.png")

# black_attack_norm versus black_defend_norm
wa = df_attack.plot(kind='scatter', x='b_attack_norm', y='b_defend_norm', color='blue', label='attacking')
wd = df_defend.plot(kind='scatter', x='b_attack_norm', y='b_defend_norm', color='red', label='defending', ax=wa)
plt.plot(centroids[0][2], centroids[0][3], 'bo')
plt.annotate("Centroid 1", (centroids[0][2], centroids[0][3]))
plt.plot(centroids[1][2], centroids[1][3], 'ro')
plt.annotate("Centroid 2", (centroids[1][2], centroids[1][3]))
plt.legend()

plt.savefig("b_attack_normVSb_defend_norm.png")

# evals_norm versus w_attack_norm
wa = df_attack.plot(kind='scatter', x='evals_norm', y='w_attack_norm', color='blue', label='attacking')
wd = df_defend.plot(kind='scatter', x='evals_norm', y='w_attack_norm', color='red', label='defending', ax=wa)
plt.plot(centroids[0][4], centroids[0][0], 'bo')
plt.annotate("Centroid 1", (centroids[0][4], centroids[0][0]))
plt.plot(centroids[1][4], centroids[1][0], 'ro')
plt.annotate("Centroid 2", (centroids[1][4], centroids[1][0]))
plt.legend()

plt.savefig("evals_normVSw_attack_norm.png")

# evals_norm versus w_defend_norm
wa = df_attack.plot(kind='scatter', x='evals_norm', y='w_defend_norm', color='blue', label='attacking')
wd = df_defend.plot(kind='scatter', x='evals_norm', y='w_defend_norm', color='red', label='defending', ax=wa)
plt.plot(centroids[0][4], centroids[0][1], 'bo')
plt.annotate("Centroid 1", (centroids[0][4], centroids[0][1]))
plt.plot(centroids[1][4], centroids[1][1], 'ro')
plt.annotate("Centroid 2", (centroids[1][4], centroids[1][1]))
plt.legend()

plt.savefig("evals_normVSw_defend_norm.png")

# evals_norm versus b_attack_norm
wa = df_attack.plot(kind='scatter', x='evals_norm', y='b_attack_norm', color='blue', label='attacking')
wd = df_defend.plot(kind='scatter', x='evals_norm', y='b_attack_norm', color='red', label='defending', ax=wa)
plt.plot(centroids[0][4], centroids[0][2], 'bo')
plt.annotate("Centroid 1", (centroids[0][4], centroids[0][2]))
plt.plot(centroids[1][4], centroids[1][2], 'ro')
plt.annotate("Centroid 2", (centroids[1][4], centroids[1][2]))
plt.legend()

plt.savefig("evals_normVSb_attack_norm.png")

# evals_norm versus b_defend_norm
wa = df_attack.plot(kind='scatter', x='evals_norm', y='b_defend_norm', color='blue', label='attacking')
wd = df_defend.plot(kind='scatter', x='evals_norm', y='b_defend_norm', color='red', label='defending', ax=wa)
plt.plot(centroids[0][4], centroids[0][3], 'bo')
plt.annotate("Centroid 1", (centroids[0][4], centroids[0][3]))
plt.plot(centroids[1][4], centroids[1][3], 'ro')
plt.annotate("Centroid 2", (centroids[1][4], centroids[1][3]))
plt.legend()

plt.savefig("evals_normVSb_defend_norm.png")

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
