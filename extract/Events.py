from extract.MapHelper import MapEvents


class Events:

    header = None

    def __init__(self,rdd,dictionary):
        self.rdd = rdd
        self.dictionary = dictionary

    def map_events(self):
        events_fields = MapEvents.get_event_fields(self.rdd)
        header = events_fields.first()[1]
        mapped_events = MapEvents.map_events(events_fields,self.dictionary,header)
        return mapped_events.groupByKey()
