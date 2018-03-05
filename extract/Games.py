from extract.MapHelper import MapGames


class Games:

    def __init__(self, rdd):
        self.rdd = rdd

    def map_games(self):
        game_fields = MapGames.get_game_fields(self.rdd)
        mapped_games = MapGames.map_games(game_fields)
        return mapped_games
