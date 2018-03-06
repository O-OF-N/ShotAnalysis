from extract.MapHelper import MapEvents
from pyspark import RDD


class Events:

    header = None

    def __init__(self,rdd,dictionary):
        self.rdd = rdd
        self.dictionary = dictionary

    def map_events(self):
        if self.rdd is None or self.dictionary is None:
            raise Exception("Invalid RDD or Dictionary")
        if not isinstance(self.rdd, RDD):
            raise Exception("RDD passed is not an instance of RDD")
        if not isinstance(self.dictionary,dict):
            raise Exception("Dictionary passed is not an instance of dictionary")
        events_fields = MapEvents.get_event_fields(self.rdd)
        if events_fields is None or not isinstance(events_fields,RDD):
            raise Exception("events_fields is None or not an instance of RDD")
        header = events_fields.first()[1]
        mapped_events = MapEvents.map_events(events_fields,self.dictionary,header)
        if mapped_events is None or not isinstance(mapped_events,RDD):
            raise Exception("events_fields is None or not an instance of RDD")
        return mapped_events.groupByKey()
