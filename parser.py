import chess.pgn
from pymongo import MongoClient
import time

start = time.time()

games = []

pgn = open("lichess_db_standard_rated_2013-01.pgn")
while True:
    line = pgn.readline().strip()
    if line == '':
        break
    state = []
    game = chess.pgn.read_game(pgn)
    board = game.board().fen()
    state.append(board)
    games.append(state)

end = time.time()
print("Time to parse:", end - start, "seconds")

print("Uploading to Mongo...")
client = MongoClient("mongodb://34.70.135.10:27017")
db = client["games"]
col = db.col
for g in games:
    for s in g:
        print("Adding position ", s)
        d = {"position": s}
        col.insert_one(d)
client.close()
print("Finished")
