import chess.pgn
from pymongo import MongoClient
import time

start = time.time()

games = []

pgn = open("example.pgn")
while True:
    line = pgn.readline().strip()
    if line == '':
        break
    state = []
    game = chess.pgn.read_game(pgn)
    board = game.board()
    state.append(board.copy())
    for move in game.mainline_moves():
        board.push(move)
        state.append(board.copy())
    games.append(state)

end = time.time()
print("Time to parse:", end - start, "seconds")

print("Uploading to Mongo...")
client = MongoClient("mongodb://34.70.135.10:27017")
db = client["games"]
col = db.col
d = { "game": games }
col.insert_one(d)
client.close()
print("Finished")
