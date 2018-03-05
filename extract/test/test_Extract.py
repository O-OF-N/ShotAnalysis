import pytest
from extract.Extract import Extract
from context.JobContext import JobContext
from util.ReadDictionary import ReadDictionary
from mock import patch
from extract.Events import Events
from extract.Games import Games


def test_extract_empty_paths():
    with pytest.raises(BaseException):
        Extract("","","")


def test_extract_empty_dictionary_paths():
    with pytest.raises(BaseException):
        Extract("","random2","random3")


def test_extract_empty_events_path():
    with pytest.raises(BaseException):
        Extract("random1","","random3")


def test_extract_empty_game_path():
    with pytest.raises(BaseException):
        Extract("random1","random2","")


def test_extract_success(spark_context):
    extract = Extract("random1", "random2", "random3")
    sc = spark_context
    with patch.object(JobContext, 'getsparkcontext') as mock_context:
        with patch.object(extract, 'get_game_rdd') as mock_game_rdd:
            with patch.object(extract, 'get_event_rdd') as mock_event_rdd:
                with patch.object(ReadDictionary, "read") as mock_dictionary:
                    with patch.object(Events, "map_events") as mock_event:
                        with patch.object(Games, "map_games") as mock_game:
                            events = sc.parallelize([5,6,7,8])
                            games = sc.parallelize([1, 2, 3, 4])
                            events_mapped = sc.parallelize([('a',1),('b',2),('c',3)])
                            games_mapped = sc.parallelize([('a',4),('b',5),('c',6)])
                            mock_event_rdd.return_value = events
                            mock_game_rdd.return_value = games
                            mock_context.return_value = sc
                            mock_dictionary.return_value = {"a":1,"b":2}
                            mock_event.return_value = events_mapped
                            mock_game.return_value = games_mapped
                            result = extract.extract().collect()
                            assert len(result) == 3
                            for r in result:
                                if r[0] == 'a':
                                    assert r[1] == (4,1)
                                elif r[0] == 'b':
                                    assert r[1] == (5,2)
                                elif r[0] == 'c':
                                    assert r[1] == (6,3)


def test_extract_empty_game_rdd(spark_context):
    extract = Extract("random1", "random2", "random3")
    sc = spark_context
    with patch.object(JobContext, 'getsparkcontext') as mock_context:
        with patch.object(extract, 'get_game_rdd') as mock_game_rdd:
            with pytest.raises(Exception):
                mock_context.return_value = sc
                mock_game_rdd.return_value = None
                extract.extract()


def test_extract_empty_event_rdd(spark_context):
    extract = Extract("random1", "random2", "random3")
    sc = spark_context
    with patch.object(JobContext, 'getsparkcontext') as mock_context:
        with patch.object(extract, 'get_game_rdd') as mock_game_rdd:
            with patch.object(extract, 'get_event_rdd') as mock_event_rdd:
                with pytest.raises(Exception):
                    events = sc.parallelize([5, 6, 7, 8])
                    mock_context.return_value = sc
                    mock_game_rdd.return_value = events
                    mock_event_rdd.return_value = None
                    extract.extract()

def test_extract_none_sparkcontext():
    extract = Extract("random1", "random2", "random3")
    sc = None
    with patch.object(JobContext, 'getsparkcontext') as mock_context:
        with pytest.raises(Exception):
            mock_context.return_value = sc
            extract.extract()


def test_extract_invalid_sparkcontext():
    extract = Extract("random1", "random2", "random3")
    sc = {}
    with patch.object(JobContext, 'getsparkcontext') as mock_context:
        with pytest.raises(Exception):
            mock_context.return_value = sc
            extract.extract()


def test_extract_invalid_dictionary(spark_context):
    extract = Extract("random1", "random2", "random3")
    sc = spark_context
    with patch.object(JobContext, 'getsparkcontext') as mock_context:
        with patch.object(ReadDictionary, "read") as mock_dictionary:
            with pytest.raises(Exception):
                mock_context.return_value = sc
                mock_dictionary.return_value = None
                extract.extract()


def test_extract_null_events(spark_context):
    extract = Extract("random1", "random2", "random3")
    sc = spark_context
    with patch.object(JobContext, 'getsparkcontext') as mock_context:
        with patch.object(extract, 'get_game_rdd') as mock_game_rdd:
            with patch.object(extract, 'get_event_rdd') as mock_event_rdd:
                with patch.object(ReadDictionary, "read") as mock_dictionary:
                    with patch.object(Events,"map_events") as mock_event:
                        with pytest.raises(Exception):
                            events = sc.parallelize([5,6,7,8])
                            games = sc.parallelize([1, 2, 3, 4])
                            mock_event_rdd.return_value = events
                            mock_game_rdd.return_value = games
                            mock_context.return_value = sc
                            mock_dictionary.return_value = {"a":1,"b":2}
                            mock_event.return_value = None
                            extract.extract()


def test_extract_success(spark_context):
    extract = Extract("random1", "random2", "random3")
    sc = spark_context
    with patch.object(JobContext, 'getsparkcontext') as mock_context:
        with patch.object(extract, 'get_game_rdd') as mock_game_rdd:
            with patch.object(extract, 'get_event_rdd') as mock_event_rdd:
                with patch.object(ReadDictionary, "read") as mock_dictionary:
                    with patch.object(Events, "map_events") as mock_event:
                        with patch.object(Games, "map_games") as mock_game:
                            with pytest.raises(Exception):
                                events = sc.parallelize([5,6,7,8])
                                games = sc.parallelize([1, 2, 3, 4])
                                events_mapped = sc.parallelize([('a',1),('b',2),('c',3)])
                                mock_event_rdd.return_value = events
                                mock_game_rdd.return_value = games
                                mock_context.return_value = sc
                                mock_dictionary.return_value = {"a":1,"b":2}
                                mock_event.return_value = events_mapped
                                mock_game.return_value = None
                                extract.extract()