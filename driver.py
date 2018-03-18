import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.extract.Extract import Extract
from src.transform.Transform import Transform

dictionary_path = "resources/dictionary.txt"
events_path = "resources/events.csv"
game_info_path = "resources/ginf.csv"

rdd_extract = Extract(dictionary_path,events_path,game_info_path).extract()
print(rdd_extract.collect())
rdd_transform = Transform(rdd_extract).transform()
