from extract.Events import Events
from extract.Games import Games
from context.JobContext import JobContext
from util.ReadDictionary import ReadDictionary
from pyspark import SparkContext, RDD


class Extract:

    broadcast_dictionary = "dictionary"

    def __init__(self, dictionary_path, events_path, game_path):
        if len(dictionary_path) < 1 or len(events_path) < 1 or len(game_path) < 1:
            raise BaseException("Invalid path")
        self.dictionary_path = dictionary_path
        self.events_path = events_path
        self.game_path = game_path

    @staticmethod
    def get_games(rdd):
        games = Games(rdd)
        if games is None:
            raise Exception("Invalid Games")
        return games

    @staticmethod
    def __get_context():
        sc = JobContext.getsparkcontext()
        if sc is None or not isinstance(sc, SparkContext):
            raise Exception("Invalid spark context")
        return sc

    @staticmethod
    def get_events(rdd, dictionary):
        events = Events(rdd, dictionary)
        if events is None:
            raise Exception("Invalid Events")
        return events

    def add_dictionary_to_broadcast(self, sc):
        dictionary_on_file = ReadDictionary(self.dictionary_path).read()
        if dictionary_on_file is None or not isinstance(dictionary_on_file, dict):
            raise Exception("Invalid mapping dictionary")
        dictionary = sc.broadcast(dictionary_on_file)
        JobContext.setBroadCast(self.broadcast_dictionary, dictionary)

    def get_dictionary_from_broadcast(self):
        return JobContext.getBoadCast(self.broadcast_dictionary)

    def get_event_rdd(self, sc):
        rdd = sc.textFile(self.events_path)
        if rdd is None or not isinstance(rdd, RDD):
            raise Exception("Invalid event rdd")
        return rdd

    def get_game_rdd(self, sc):
        rdd = sc.textFile(self.game_path)
        if rdd is None or not isinstance(rdd, RDD):
            raise Exception("Invalid game rdd")
        return rdd

    def extract(self):
        try:
            sc = self.__get_context()
            self.add_dictionary_to_broadcast(sc)
            game_rdd = self.get_game_rdd(sc)
            event_rdd = self.get_event_rdd(sc)
            dictionary = self.get_dictionary_from_broadcast()
            events = self.get_events(event_rdd, dictionary).map_events()
            games = self.get_games(game_rdd).map_games()
            if events is None or games is None:
                raise Exception("Invalid events or games rdd")
            game_events = games.join(events)
            return game_events
        except Exception as ex:
            raise ex
