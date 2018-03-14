from src.extract.MapHelper import MapGames
from pyspark import RDD


class Games:

    def __init__(self, rdd):
        self.rdd = rdd

    def map_games(self):
        if self.rdd is None or not isinstance(self.rdd, RDD):
            raise Exception("Invalid RDD")
        game_fields = MapGames.get_game_fields(self.rdd)
        if game_fields is None or not isinstance(game_fields, RDD):
            raise Exception("game_fields is None or not an instance of RDD")
        mapped_games = MapGames.map_games(game_fields)
        if mapped_games is None or not isinstance(mapped_games, RDD):
            raise Exception("mapped_games is None or not an instance of RDD")
        return mapped_games
