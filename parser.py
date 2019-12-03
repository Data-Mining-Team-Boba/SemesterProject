import chess.pgn
from pymongo import MongoClient

state = []

pgn = open("example.pgn")
game = chess.pgn.read_game(pgn)
board = game.board()
state.append(board.copy())
for move in game.mainline_moves():
    board.push(move)
    state.append(board.copy())

client = MongoClient("mongodb://34.70.135.10:27017")
client.close()
