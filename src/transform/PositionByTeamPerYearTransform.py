from src.transform.TransformBase import TransformBase
from src.models.NamedTuples import PositionByTeamPerYear,Count

class PositionByTeamPerYearTransform(TransformBase):

  def __init__(self,rdd):
    super(PositionByTeamPerYearTransform,self).__init__(rdd)

  @classmethod
  def getPositionByTeamPerYear(self,r):
    game = r[1][0]
    event = r[1][1]
    goal_count = 1 if event.isGoal == True else 0
    return ((event.eventTeam,game.season,event.location), (1,goal_count))

  @classmethod
  def aggregateCount(self,r1, r2):
    count = r1[0] + r2[0]
    goal_count = r1[1] + r2[1]
    return (count,goal_count)


  def transform(self):
    position_by_team_per_year = self.rdd.map(self.getPositionByTeamPerYear)
    position_by_team_per_year_1 = position_by_team_per_year.reduceByKey(self.aggregateCount)
    print(position_by_team_per_year_1.collect())
    