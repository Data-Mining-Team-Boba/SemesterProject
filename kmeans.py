## NOTE: This isn't working... Moved to scala instead because documentation was better

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

from math import sqrt
import statistics
import chess.pgn
import chess
import datetime

from pyspark.mllib.clustering import KMeans, KMeansModel

def transformChessData(row):
    positions = []
    for position in row["game_fen_positions"]:
        board = chess.Board(position)
        positions.append(board)

    pos_num = 0

    w_attack = 0
    w_defend = 0
    b_attack = 0
    b_defend = 0

    for position in positions:
        board = position
        # check number of attacking/defending positions after both sides have played
        for square in chess.SQUARES:
            if (str(board.piece_at(square)) != 'None'):
                attacked = board.attackers(chess.WHITE, square)
                for attack in attacked:
                    if (str(board.piece_at(attack)) != 'None'):
                        # if a piece is white and is attacking a black piece
                        if (board.piece_at(square).color == False):
                            w_attack += 1
                            # print("%s attacking %s" % (board.piece_at(attack), board.piece_at(square)))
                        # if a piece is white and is attacking a white piece
                        elif (board.piece_at(square).color == True):
                            w_defend += 1
                            # print("%s defending %s" % (board.piece_at(attack), board.piece_at(square)))
                attacked = []
                attacked = board.attackers(chess.BLACK, square)
                for attack in attacked:
                    if (str(board.piece_at(attack)) != 'None'):
                        # if a piece is black and is attacking a white piece
                        if (board.piece_at(square).color == True):
                            b_attack += 1
                            # print("%s attacking %s" % (board.piece_at(attack), board.piece_at(square)))
                        elif (board.piece_at(square).color == False):
                            b_defend += 1
                            # print("%s defending %s" % (board.piece_at(attack), board.piece_at(square)))
                attacked = []
        pos_num += 1

    return Vectors.dense(w_attack / pos_num, w_defend / pos_num, b_attack / pos_num, b_defend / pos_num, statistics.mean(row["evals"]))


sc = SparkContext()
spark = SparkSession \
    .builder \
    .appName("ChessKMeans") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/chess_data_3.games?readPreference=primaryPreferred") \
    .getOrCreate()

df = spark.read.format("mongo").load()

# Parse game data and generate numeric values so we can feed it into kmeans
parsedData = df.rdd.map(transformChessData)


# Build the model (cluster the data)
kmeans = KMeans()
model = kmeans.train(parsedData, k=2, maxIterations=100)

model.save(sc, "KMeansModel")
for center in model.centers:
    print(center)




# center = model.centers

# model = kmeans.fit(training)
# print(model)

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