import pytest
from mock import patch
from extract.Games import Games
from extract.MapHelper import MapGames


def test_map_games_none_rdd():
    games = Games(None)
    with pytest.raises(Exception):
        games.map_games()


def test_map_games_invalid_rdd():
    games = Games({})
    with pytest.raises(Exception):
        games.map_games()


def test_map_games_success(spark_context):
    rdd = spark_context.parallelize([1, 2, 3])
    rdd1 = spark_context.parallelize([('a', 4), ('b', 5), ('c', 6)])
    rdd2 = spark_context.parallelize([('a', 7), ('b', 8), ('c', 9)])
    games = Games(rdd)
    with patch.object(MapGames, 'get_game_fields') as mock_game_fields:
        with patch.object(MapGames, 'map_games') as mock_map_games:
            mock_game_fields.return_value = rdd1
            mock_map_games.return_value = rdd2
            result = games.map_games().collect()
            print(result)
            assert len(result) == 3
            for r in result:
                if r[0] == 'a':
                    assert r[1] == 7
                elif r[0] == 'b':
                    assert r[1] == 8
                elif r[0] == 'c':
                    assert r[1] == 9


def test_map_games_none_eventfields(spark_context):
    rdd = spark_context.parallelize([1, 2, 3])
    games = Games(rdd)
    with patch.object(MapGames, 'get_game_fields') as mock_game_fields:
        with pytest.raises(Exception):
            mock_game_fields.return_value = None
            games.map_games().collect()


def test_map_games_eventfields_notrdd(spark_context):
    rdd = spark_context.parallelize([1, 2, 3])
    games = Games(rdd)
    with patch.object(MapGames, 'get_game_fields') as mock_game_fields:
        with pytest.raises(Exception):
            mock_game_fields.return_value = {}
            games.map_games().collect()


def test_map_games_none_mappedevents(spark_context):
    rdd = spark_context.parallelize([1, 2, 3])
    rdd1 = spark_context.parallelize([('a', 4), ('b', 5), ('c', 6)])
    games = Games(rdd)
    with patch.object(MapGames, 'get_game_fields') as mock_game_fields:
        with patch.object(MapGames, 'map_games') as mock_map_games:
            with pytest.raises(Exception):
                mock_game_fields.return_value = rdd1
                mock_map_games.return_value = None
                games.map_games().collect()


def test_map_games_mappedevents_notrdd(spark_context):
    rdd = spark_context.parallelize([1, 2, 3])
    rdd1 = spark_context.parallelize([('a', 4), ('b', 5), ('c', 6)])
    games = Games(rdd)
    with patch.object(MapGames, 'get_game_fields') as mock_game_fields:
        with patch.object(MapGames, 'map_games') as mock_map_games:
            with pytest.raises(Exception):
                mock_game_fields.return_value = rdd1
                mock_map_games.return_value = {}
                games.map_games().collect()
