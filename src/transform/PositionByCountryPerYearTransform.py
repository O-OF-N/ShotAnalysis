from src.transform.TransformBase import TransformBase
from src.models.NamedTuples import PositionByCountryPerYear

class PositionByCountryPerYearTransform(TransformBase):

  def __init__(self,rdd):
    super(PositionByCountryPerYearTransform,self).__init__(rdd)

  def transform(self):
    print(self.rdd)