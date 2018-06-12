from src.transform.PositionByTeamPerYearTransform import PositionByTeamPerYearTransform

class Transform:

  def __init__(self, extract_path):
    self.extract_path = extract_path

  def transform(self):
    transform1 = PositionByTeamPerYearTransform(self.extract_path)
    transform1.transform()