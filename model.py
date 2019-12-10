import pandas as pd
import matplotlib.pyplot as pyplot
from sklearn.cluster import KMeans
import chess
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["chess_data"]
games_collection = db["games_collection"]
games = games_collection.find()

df = pd.DataFrame()
frames = []

for game in games:
	frames.append(game)

res = pd.concat(frames)
	
kmeans = KMeans(n_clusters=3).fit(res)
