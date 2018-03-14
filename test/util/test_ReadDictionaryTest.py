import pytest
from src.util.ReadDictionary import ReadDictionary


def test_read():
    dict1 = ReadDictionary(path="test/util/dictionary.txt").read()
    assert len(dict1) == 66


def test_read_path_empty():
    dict1 = ReadDictionary(path="").read()
    assert len(dict1) == 0


def test_read_path_non_existent():
    dict1 = ReadDictionary(path="test/util/dictionary1.txt").read()
    assert len(dict1) == 0


def test_read_empty_file():
    dict1 = ReadDictionary(path="test/util/dictionary_empty.txt").read()
    assert len(dict1) == 0