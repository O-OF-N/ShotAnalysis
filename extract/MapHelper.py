from functools import partial
from collections import namedtuple


class MapEvents:
    Event = namedtuple("Event", "eventTeam opponent eventId location isGoal shotOutcome")

    @classmethod
    def __map_rdd_dictionary(self,dictionary, header, element):
        if element == header:
            return [header]
        else:
            return [self.Event(eventTeam= element[7], opponent= element[8], eventId=element[0],
                               location=self.__get_from_dict(dictionary,header[16],element[16]),
                               isGoal=False if element[15] is 0 else True,
                               shotOutcome=self.__get_from_dict(dictionary,header[14],element[14]))]
    @classmethod
    def __get_from_dict(self, dictionary, field,key):
        try:
            return dictionary.value[field+"_"+key]
        except:
            return key

    @classmethod
    def map_events(cls,events_fields, dictionary, header):
        map_with_dictionary = partial(cls.__map_rdd_dictionary, dictionary, header)
        return events_fields.mapValues(map_with_dictionary)

    @classmethod
    def get_event_fields(cls, rdd):
        return rdd.map(lambda x: (x.split(",")[0],tuple(x.split(",")[1:])))


class MapGames:
    Game = namedtuple("Game", "homeTeam visitingTeam season")

    @classmethod
    def __parse_game(cls, game):
        return cls.Game(homeTeam=game[6],visitingTeam=game[7],season=game[4])

    @classmethod
    def map_games(cls, games_fields):
        return games_fields.mapValues(cls.__parse_game)

    @classmethod
    def get_game_fields(cls, rdd):
        return rdd.map(lambda x:(x.split(",")[0],tuple(x.split(",")[1:])))
