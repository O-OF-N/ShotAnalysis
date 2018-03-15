import pytest
from mock import patch
from src.extract.MapHelper import MapGames
from src.extract.MapHelper import MapEvents

# MapEvents:get_event_fields


def test_get_event_fields(spark_context):
    rdd = spark_context.parallelize(
        ["1,2,3,4,5,6,7", "10,20,30,40,50,60,70", "100,200,300,400,500,600,700"])
    rdd_new = MapEvents.get_event_fields(rdd).collect()
    for r in rdd_new:
        if r[0] == "1":
            assert r[1] == ("2", "3", "4", "5", "6", "7")
        if r[0] == "10":
            assert r[1] == ("20", "30", "40", "50", "60", "70")
        if r[0] == "100":
            assert r[1] == ("200", "300", "400", "500", "600", "700")


def test_get_event_fields_empty_rdd(spark_context):
    rdd = spark_context.emptyRDD()
    rdd_new = MapEvents.get_event_fields(rdd).collect()
    assert len(rdd_new) == 0


def test_get_event_fields_null_rdd():
    rdd = None
    rdd_new = MapEvents.get_event_fields(rdd)
    assert rdd_new is None


def test_get_event_fields_not_rdd():
    rdd = {}
    rdd_new = MapEvents.get_event_fields(rdd)
    assert rdd_new is None


# MapEvents:map_events

def test_get_mapped_events(spark_context):
    rdd = spark_context.parallelize(["1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20", 
        "10,20,30,40,50,60,70,80,90,100,110,120,130,140,150,160,170,180,190,200", 
        "100,200,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,2000"])
    event_fields = MapEvents.get_event_fields(rdd)
    header = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
    mapped_events = MapEvents.map_events(event_fields,{},header).collect()
    assert(len(mapped_events) == 3)


def test_get_mapped_events_event_fields_none(spark_context):
    header = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
    mapped_events = MapEvents.map_events(None,{},header)
    assert(mapped_events == None)


def test_get_mapped_events_event_fields_not_rdd(spark_context):
    header = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
    mapped_events = MapEvents.map_events({},{},header)
    assert(mapped_events == None)

def test_get_mapped_events_event_fields_header_none(spark_context):
    rdd = spark_context.parallelize(["1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20", 
        "10,20,30,40,50,60,70,80,90,100,110,120,130,140,150,160,170,180,190,200", 
        "100,200,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,2000"])
    event_fields = MapEvents.get_event_fields(rdd)
    header = None
    mapped_events = MapEvents.map_events(event_fields,{},header)
    assert(mapped_events == None)

def test_get_mapped_events_event_fields_dictionary_none(spark_context):
    rdd = spark_context.parallelize(["1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20", 
        "10,20,30,40,50,60,70,80,90,100,110,120,130,140,150,160,170,180,190,200", 
        "100,200,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,2000"])
    event_fields = MapEvents.get_event_fields(rdd)
    header = None
    mapped_events = MapEvents.map_events(event_fields,None,header)
    assert(mapped_events == None)


# MapGames:get_game_fields
def test_get_game_fields(spark_context):
    rdd = spark_context.parallelize(
        ["1,2,3,4,5,6,7", "10,20,30,40,50,60,70", "100,200,300,400,500,600,700"])
    rdd_new = MapGames.get_game_fields(rdd).collect()
    assert(len(rdd_new) == 3)


def test_get_game_fields_empty_rdd(spark_context):
    rdd = spark_context.emptyRDD()
    rdd_new = MapGames.get_game_fields(rdd).collect()
    assert len(rdd_new) == 0


def test_get_game_fields_null_rdd():
    rdd = None
    rdd_new = MapGames.get_game_fields(rdd)
    assert rdd_new is None


def test_get_game_fields_not_rdd():
    rdd = {}
    rdd_new = MapGames.get_game_fields(rdd)
    assert rdd_new is None

# MapGames:map_games


def test_get_mapped_game(spark_context):
    rdd = spark_context.parallelize(
        ["1,2,3,4,5,6,7,8,9", "10,20,30,40,50,60,70,80,90", "100,200,300,400,500,600,700,800,900"])
    game_fields = MapGames.get_game_fields(rdd)
    mapped_games = MapGames.map_games(game_fields).collect()
    for r in mapped_games:
        if r[0] == "1":
            assert r[1].homeTeam == "8"
            assert r[1].visitingTeam == "9"
            assert r[1].season == "6"
        if r[0] == "10":
            assert r[1].homeTeam == "80"
            assert r[1].visitingTeam == "90"
            assert r[1].season == "60"
        if r[0] == "100":
            assert r[1].homeTeam == "800"
            assert r[1].visitingTeam == "900"
            assert r[1].season == "600"


def test_get_mapped_game_none_rdd(spark_context):
    mapped_games = MapGames.map_games(None)
    assert mapped_games is None


def test_get_mapped_game_not_rdd(spark_context):
    mapped_games = MapGames.map_games({})
    assert mapped_games is None


def test_get_mapped_game_invalid_len(spark_context):
    rdd = spark_context.parallelize(
        ["1,2,3,4,5,6", "10,20,30,40,50,60", "400,500,600,700,800,900"])
    game_fields = MapGames.get_game_fields(rdd)
    mapped_games = MapGames.map_games(game_fields).collect()
    for r in mapped_games:
        if r[0] == "1":
            assert r[1] is None
        if r[0] == "10":
            assert r[1] is None
        if r[0] == "100":
            assert r[1] is None
