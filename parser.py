import chess.pgn
from pymongo import MongoClient
import time
import sys
import concurrent.futures
import threading
import pickle
import multiprocessing


def boardTo2dMatrix(board):
    rows = str(board).split("\n")

    retMatrix = []
    for row in rows:
        retMatrix.append(row.split())

    return retMatrix

def processPGNGame(game, i, start):
    client = MongoClient("mongodb://localhost:27017/", maxPoolSize=None) # This is inefficient but should be fine for now... Ideally we want to do bulk uploads instead of one at a time
    db = client["chess_data"]
    games_collection = db["games_collection"]

    # Grab the initial board state
    board = game.board()
    fenPositions = [board.fen()]
    matrixPositions = [boardTo2dMatrix(board)]

    # Add the position of every move
    for move in game.mainline_moves():
        board.push(move)
        fenPositions.append(board.fen())

        matrixPositions.append(boardTo2dMatrix(board))

    # Upload the game to Mongo
    game_data = {"game_fen_positions" : fenPositions, "game_matrix_positions": matrixPositions}
    games_collection.insert_one(game_data)

    if i % 1000 == 0:
        print("({}) Time elapsed: {} seconds".format(i, time.time() - start), flush=True)
    exit(0)
    # return game_data

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Incorrect arguments given. Run with the format `python3 parser.py <file>`")
        exit(-1)

    sys.setrecursionlimit(2000) # Need to increase because some games are super long so pickling reaches recursion limit
    start = time.time()

    # Process pool executor for parallel computing of games
    max_processes = multiprocessing.cpu_count()*3
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=max_processes)

    fp = open(sys.argv[1])

    i = 0
    while True:
        line = fp.readline().strip()
        if line == '':
            break
        i+=1

        # Read in pgn format into a game object
        game = chess.pgn.read_game(fp)

        args = {"game": game, "i": i, "start": start}
        executor.submit(processPGNGame, **args)

        # if i == 10:
        #     break


    end = time.time()
    print("Time to parse:", end - start, "seconds")
    # time.sleep(10) # Let all processes finish up before closing the file
    fp.close()
