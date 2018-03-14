import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from extract.Extract import Extract

out = Extract("/Users/vm033450/Downloads/football-events/dictionary.txt",
        "/Users/vm033450/Downloads/football-events/events.csv",
        "/Users/vm033450/Downloads/football-events/ginf.csv").extract()
print(out.collect())
