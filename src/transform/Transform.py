from src.transform.PositionByTeamPerYearTransform import PositionByTeamPerYearTransform

class Transform:

  def __init__(self, rdd):
    self.rdd = rdd

  def transform(self):
    print('inside transform......')
    transform1 = PositionByTeamPerYearTransform(self.rdd)
    transform1.transform()