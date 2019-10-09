"""SimpleApp.py"""
from pyspark.sql import SparkSession

logFile = "/home/user/Work/Applications/spark-2.4.3-bin-hadoop2.7/examples/src/main/resources/people.txt"  # Should be some file on your system
spark = SparkSession.builder.appName("Sample Python App").master("spark://localhost:7077").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()