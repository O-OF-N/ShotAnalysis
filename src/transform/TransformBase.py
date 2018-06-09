
from abc import ABC, abstractmethod

class TransformBase(ABC):

  #rdd = None

  @abstractmethod
  def __init__(self,rdd):
    print(rdd)
    self.rdd = rdd

  @abstractmethod
  def transform(self):
    pass