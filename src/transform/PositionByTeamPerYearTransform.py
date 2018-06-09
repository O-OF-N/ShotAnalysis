from src.transform.TransformBase import TransformBase
from src.models.NamedTuples import PositionByTeamPerYear

class PositionByTeamPerYearTransform(TransformBase):

  def __init__(self,rdd):
    super(PositionByTeamPerYearTransform,self).__init__(rdd)

  #def getPositionByTeamPerYear():


  def transform(self):
    print(self.rdd.collect())
    #return self.rdd.map(lambda r: PositionByTeamPerYear(r.eventTeam,r.year,r.location,r.))
    