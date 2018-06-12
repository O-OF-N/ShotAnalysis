from collections import namedtuple


# Extract
Event = namedtuple("Event", "eventTeam opponent eventId location isGoal shotOutcome")
Game = namedtuple("Game", "homeTeam visitingTeam season")


# Transform

Count = namedtuple("Count", "positionCount goalCount")
PositionByTeamPerYear = namedtuple("PositionByTeamPerYear", "team year location isGoal")
PositionByCountryPerYear = namedtuple("PositionByCountryPerYear", "country year location isGoal")