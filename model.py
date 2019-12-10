import pandas as pd
import matplotlib.pyplot as pyplot
from sklearn.cluster import KMeans
import chess.pgn
import chess
from pymongo import MongoClient
from collections import Counter

client = MongoClient("mongodb://localhost:27017/")
db = client["chess_data"]
games_collection = db["games_collection"]
games = games_collection.find()
positions = []

for game in games:
	for position in game:
		positions.append(position)

x, y = [], []

for position in positions:
	num_attackers = 0
	num_attacked = 0
	for square in chess.SQUARES:
		attacks = board.attacks(square)
		num_attacked += len(attacks)
		attacked_cnt = 0
		for attacked_square in chess.SQUARES:
			attacked = board.is_attacked_by(chess.WHITE, attacked_square)
			if(attacked):
				num_attackers += 1
			attacked = board.is_attacked_by(chess.BLACK, attacked_square)
			if(attacked):
				num_attackers += 1
	x.append(num_attacked)
	y.append(num_attackers)

d = {'x': x, 'y': y}
df = pd.DataFrame(d,columns=['x','y'])

kmeans = KMeans(n_clusters=3).fit(df)
