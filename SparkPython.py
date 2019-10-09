from pyspark import SparkContext, SparkConf


class SparkPython:
    def __init__(self, *args, **kwargs):
        app_name = kwargs.get("appName", None)
        master = kwargs.get("master", None)
        conf = SparkConf().setAppName(app_name).setMaster(master)
        self.sc = SparkContext(conf=conf)

    def start_spark(self):
        data = [1, 2, 3, 4, 5]
        distData = self.sc.parallelize(data)
        print(distData)


if __name__ == "__main__":
    init_data = {"appName": "Spark Python Work", "master": "spark://localhost:7077"}
    sp = SparkPython(**init_data)
    sp.start_spark()