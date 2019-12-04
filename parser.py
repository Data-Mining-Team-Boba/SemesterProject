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
    board = game.board()
    state.append(board.copy())
    for move in game.mainline_moves():
        board.push(move)
        state.append(board.copy())
    games.append(state)

end = time.time()
print("Time to parse file: ", end - start)

client = MongoClient("mongodb://34.70.135.10:27017")
client.close()
