import chess.pgn
from pymongo import MongoClient
import time
import sys

if __name__ == "__main__":
	if len(sys.argv) < 3:
		print("Incorrect arguments given. Run with the format `python3 parser.py <file> <label>`")
		exit(-1)

	filename = sys.argv[1]
	label = sys.argv[2]

	client = MongoClient("mongodb://localhost:27017/", maxPoolSize=None)
	db = client["chess_testing_data"]
	games_collection = db["games"]

	fenPositions = []
	posEvals = []

	# Go through each line in the file and populate the fenPositions and posEvals list
	fp = open(sys.argv[1])
	while True:
		line = fp.readline().strip()
		if line == '':
			break

		lineArr = line.split(',')
		fenPositions.append(lineArr[0])
		posEvals.append(float(lineArr[1]))

	game_data = {"filename": filename, "label": label, "game_fen_positions": fenPositions, "evals": posEvals}
	games_collection.insert_one(game_data)

	print("{} uploaded".format(filename))