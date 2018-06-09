from pyspark import SparkContext,SparkConf


class JobContext:

    __broadcast = {}
    __sc = None
    __conf = SparkConf().setMaster("local").setAppName("Football-Analysis")

    @classmethod
    def getsparkcontext(cls):
        if not cls.__sc == None:
            return cls.__sc.getOrCreate(cls.__conf)

        cls.__sc = SparkContext()
        return cls.__sc.getOrCreate(cls.__conf)

    @classmethod
    def setBroadCast(cls,key,value):
        cls.__broadcast[key] = value

    @classmethod
    def getBoadCast(cls,key):
        return cls.__broadcast[key]

    @classmethod
    def stop(cls):
        cls.__sc.stop()
    