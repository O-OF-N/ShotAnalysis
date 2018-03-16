from pyspark import SparkContext,SparkConf


class JobContext:

    __broadcast = {}
    __sc = None

    @classmethod
    def getsparkcontext(cls):
        __conf = SparkConf().setMaster("local").setAppName("Football-Analysis")
        if not cls.__sc == None:
            return cls.__sc.getOrCreate(__conf)

        cls.__sc = SparkContext()
        return cls.__sc.getOrCreate(__conf)

    @classmethod
    def setBroadCast(cls,key,value):
        cls.__broadcast[key] = value

    @classmethod
    def getBoadCast(cls,key):
        return cls.__broadcast[key]

    @classmethod
    def stop(cls):
        cls.__sc.stop()
    