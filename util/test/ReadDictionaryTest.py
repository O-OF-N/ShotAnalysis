import unittest
from util.ReadDictionary import ReadDictionary
class ReadDictionaryTest(unittest.TestCase):

    def test_read(self):
        dict = ReadDictionary.read(path="dictionary.txt")
        assert len(dict) == 66

    def test_read_path_empty(self):
        dict = ReadDictionary.read(path="")
        assert len(dict) == 0

    def test_read_path_non_existent(self):
        dict = ReadDictionary.read(path="dictionary1.txt")
        assert len(dict) == 0

    def test_read_empty_file(self):
        dict = ReadDictionary.read(path="dictionary_empty.txt")
        assert len(dict) == 0

if __name__ == "__main__":
    unittest.main()