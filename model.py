import pandas as pd
import matplotlib.pyplot as pyplot
import numpy as np
from sklearn.cluster import KMeans
import chess.pgn
import chess
from pymongo import MongoClient
from collections import Counter
from pprint import pprint

# board = chess.Board('rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR')

# board.attackers(color: bool, square: int)
# gets the set of attackers of the given color for the given square

# board.attacks(square: int)
# gets the set of attacked squares from the given square

# board.is_attacked_by(chess.WHITE, attacked_square)
# checks if the given side attacks the given square

client = MongoClient("mongodb://localhost:27017/")
db = client["chess_data"]
games_collection = db["games_collection"]
games = games_collection.find()
game_ids = []
position_ids = []
was = []
wds = []
bas = []
bds = []

for game in games:
	positions = []
	wa, wd, ba, bd = [], [], [], []
	# print(game)
	for position in game["game_fen_positions"]:
		# print(position)
		board = chess.Board(position)
		positions.append(board)

	pos_num = 0

	w_attack = 0
	w_defend = 0
	b_attack = 0
	b_defend = 0
	
	for position in positions:
		# print("")
		# print(position)
		board = position
		# check number of attacking/defending positions after both sides have played
		if (pos_num % 2 == 0 and pos_num != 0):
			# w_attack = 0
			# w_defend = 0
			# b_attack = 0
			# b_defend = 0

			for square in chess.SQUARES:
				# print(board.piece_at(square))
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
			
			# attacking / defending per game per position
			# game_ids.append(game["_id"])
			# position_ids.append(pos_num)
			# was.append(w_attack)
			# wds.append(w_defend)
			# bas.append(b_attack)
			# bds.append(b_defend)
		pos_num += 1
	
	# average attacking / defending per game
	game_ids.append(game["_id"])
	was.append(w_attack / pos_num)
	wds.append(w_defend / pos_num)
	bas.append(b_attack / pos_num)
	bds.append(b_defend / pos_num)


print("final dataframe")
# print(df.head())
# print(len(df))

# d = {'game_id': game_ids, 'position_id': position_ids, 'wa': was, "wd": wds, "ba": bas, "bd": bds}
d = {'game_id': game_ids, 'wa': was, "wd": wds, "ba": bas, "bd": bds}
df = pd.DataFrame(d)
# print(df.head(5))

# normalize data
kmeans = KMeans(n_clusters = 2)
predictions = kmeans.fit_predict(np.array(df[["wa", "wd", "ba", "bd"]]))
df["cluster_num"] = predictions
# print(df.head(10))