from functools import reduce

class ReadDictionary():
    """
        This class reads the dictionary.txt file. It breaks it down in to key value pairs.
        Key is the header+"_"+key and value is the value.
        For example:
            card
            0   Red
            1 Yellow
        becomes
        {"card_0": "Red", "card_1": "yellow"}
    """
    def __init__(self,path):
        self.path = path

    def read(self):
        """
        Reads the file from path and populates the dictionary.
        :return:
        """
        dictionary = {}
        try:
            with open(self.path) as file:
                key_header = ""
                for line in file:
                    entry = line.strip().split()
                    if len(entry) == 0:
                        continue
                    if len(entry) == 1:
                        key_header = entry[0]+"_"
                    else:
                        key = entry[0].strip()
                        value = reduce(lambda x1,y1: x1+" "+ y1,entry[1:])
                        dictionary[key_header+key] = value
        except FileNotFoundError as fnf:
            print("File Not found")
        return dictionary

