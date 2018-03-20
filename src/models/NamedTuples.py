from collections import namedtuple


# Extract
Event = namedtuple("Event", "eventTeam opponent eventId location isGoal shotOutcome")
Game = namedtuple("Game", "homeTeam visitingTeam season")


# Transform

PositionByTeamPerYear = namedtuple("PositionByTeamPerYear", "team year location isGoal")
PositionByCountryPerYear = namedtuple("PositionByTeamPerYear", "country year location isGoal")