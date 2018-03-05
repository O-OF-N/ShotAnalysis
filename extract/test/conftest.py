import pytest
from pyspark import SparkContext,SparkConf


@pytest.fixture(scope="session",name="spark_context")
def spark_context():
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    conf = (SparkConf().setMaster("local").setAppName("extract-local-testing"))
    sc = SparkContext(conf=conf).getOrCreate()
    return sc
