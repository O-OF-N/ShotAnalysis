from functools import partial
from pyspark import RDD
from src.models.NamedTuples import Event, Game


class MapEvents(object):

    @classmethod
    def map_rdd_dictionary(self, dictionary, header, element):
        return Event(eventTeam=element[7],
                      opponent=element[8],
                      eventId=element[0],
                      location=self.get_from_dict(
                          dictionary, header[16], element[16]),
                      isGoal=False if element[15] is '0' else True,
                      shotOutcome=self.get_from_dict(dictionary, header[14], element[14]))

    @classmethod
    def get_from_dict(self, dictionary, field, key):
        try:
            return dictionary.value[field+"_"+key]
        except:
            return key

    @classmethod
    def map_events(cls, events_fields, dictionary, header):
        if events_fields is None or not isinstance(events_fields, RDD):
            return None
        if header is None:
            return None
        map_with_dictionary = partial(
            cls.map_rdd_dictionary, dictionary, header)
        return events_fields.mapValues(map_with_dictionary)

    @classmethod
    def get_event_fields(cls, rdd):
        if rdd is None or not isinstance(rdd, RDD):
            return None
        return rdd.map(lambda x: (x.split(",")[0], tuple(x.split(",")[1:])))


class MapGames(object):

    @classmethod
    def parse_game(cls, game):
        if len(game) < 8:
            return None
        return Game(homeTeam=game[6], visitingTeam=game[7], season=game[4])

    @classmethod
    def map_games(cls, games_fields):
        if games_fields is None or not isinstance(games_fields, RDD):
            return None
        return games_fields.mapValues(cls.parse_game)

    @classmethod
    def get_game_fields(cls, rdd):
        if rdd is None or not isinstance(rdd, RDD):
            return None
        return rdd.map(lambda x: (x.split(",")[0], tuple(x.split(",")[1:])))
