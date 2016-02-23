import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext

APP_NAME = "Group1_2_Top10AirlinesOntimeArrival"
STREAMING_INTERVAL = 1

master = str(sys.argv[1]) # 'ec2-xx-xx-xx-xx.compute-1.amazonaws.com'
dataFilePathOnHdfs = "hdfs://{}/btsdata/aviation/ontime/".format(master)

conf = SparkConf().setAppName(APP_NAME).setMaster('spark://{}:7077'.format(master))
sc = SparkContext(conf)
ssc = StreamingContext(sc, STREAMING_INTERVAL)
ssc.checkpoint('/tmp/ccc')

lines = ssc.textFileStream(dataFilePathOnHdfs)


res1_2 = lines.map(lambda line : line.split(","))				\
			  .map(lambda line : (line[4], float(line[11])))	\	# (Airline, Arrival Delay)
			  .combineByKey(lambda x : (x, 1), 					\
			  				lambda x, y : (x[0] + y, x[1] + 1), \	# (sum, count)
			  				lambda x, y : (x[0] + y[0], x[1] + y[1]) ) \
			  .map(lambda (key, (valueSum, count) : (key, valueSum / count))) \
			  .sortByKey('ascending')


ssc.start()
while true:
	if ssc.awaitTerminationOrTimeout(10):
		break
	else:
		pass
print res1_2.take(10)
print "Gracefully stopping Spark Streaming Application"
ssc.stop(stopSparkContext = True, stopGracefully = True)
print "Application stoppped"