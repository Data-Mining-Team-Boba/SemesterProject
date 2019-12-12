import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import FloatType

from math import sqrt
import statistics
import chess.pgn
import chess
import datetime

from pyspark.ml.clustering import KMeans, KMeansModel

# Normalizing the column values for a given dataframe
def normalizeData(df, columnNames):
    mins = []
    maxes = []
    for colName in columnNames:
        min_col_val = df.agg({colName: "min"}).collect()[0]["min({})".format(colName)]
        max_col_val = df.agg({colName: "max"}).collect()[0]["max({})".format(colName)]

        # normalize the column using: (x - xmin) / (xmax - xmin)
        df = df.withColumn(colName + "_norm", ((df[colName] - min_col_val) / (max_col_val - min_col_val)))

    return df

# Extracting numeric values for KMeans
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

    # return Vectors.dense(b_attack / pos_num, b_defend / pos_num, statistics.mean(row["evals"]))
    # return Vectors.dense(w_attack / pos_num, w_defend / pos_num, statistics.mean(row["evals"]))
    return Row(w_attack=w_attack / pos_num, w_defend=w_defend / pos_num, b_attack=b_attack / pos_num, b_defend=b_defend / pos_num, evals=statistics.mean(row["evals"]))

sc = SparkContext()
spark = SparkSession \
    .builder \
    .appName("ChessKMeans") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/chess_data_3.games?readPreference=primaryPreferred") \
    .getOrCreate()

# Load in all the mongo data from the database and collection defined
df = spark.read.format("mongo").load()

# Parse game data and generate numeric values so we can feed it into kmeans
parsedData = df.rdd.map(transformChessData)

# Convert PipelinedRDD to dataframe
sqlContext = SQLContext(sc)
schemaFeatures = sqlContext.createDataFrame(parsedData)

# Normalize all the columns
normalizedDF = normalizeData(schemaFeatures, ["w_attack", "w_defend", "b_attack", "b_defend", "evals"])
# normalizedDF.show()

# Combine all normalized columns into one "features" column
assembler = VectorAssembler(inputCols=["w_attack_norm", "w_defend_norm", "b_attack_norm", "b_defend_norm", "evals_norm"], outputCol="features")

training = assembler.transform(normalizedDF)

# Build the model (cluster the data)
kmeans = KMeans(k=2, maxIter=100)
model = kmeans.fit(training)

model.save("KMeansModel_final_both_norm")
for center in model.centers:
    print(center)