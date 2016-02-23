import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext

APP_NAME = "Group1_1_Top10MostPopularAirports"
STREAMING_INTERVAL = 1

master = str(sys.argv[1]) # 'ec2-xx-xx-xx-xx.compute-1.amazonaws.com'
dataFilePathOnHdfs = "hdfs://{}/btsdata/aviation/ontime/".format(master)

conf = SparkConf().setAppName(APP_NAME).setMaster('spark://{}:7077'.format(master))
sc = SparkContext(conf)
ssc = StreamingContext(sc, STREAMING_INTERVAL)
ssc.checkpoint('/tmp/ccc')

lines = ssc.textFileStream(dataFilePathOnHdfs)


res1_1 = lines.map(lambda line : line.split(","))			\
			  .map(lambda line : (line[6], line[7]))		\	# Note: (origin, destination)
			  .flatMap(lambda x : [(x[0], 1), (x[1], 1)])	\
			  .reduceByKey(lambda a, b : a + b)				\
			  .sortByKey('descending')


ssc.start()
while true:
	if ssc.awaitTerminationOrTimeout(10):
		break
	else:
		pass
print res1_1.take(10)
print "Gracefully stopping Spark Streaming Application"
ssc.stop(stopSparkContext = True, stopGracefully = True)
print "Application stoppped"