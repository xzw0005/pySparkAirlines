import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext

APP_NAME = "Group2_2_Top10DestinationsOntimeDepartureFromX"
STREAMING_INTERVAL = 1

master = str(sys.argv[1]) # 'ec2-xx-xx-xx-xx.compute-1.amazonaws.com'
originAirport = str(sys.argv[2])
dataFilePathOnHdfs = "hdfs://{}/btsdata/aviation/ontime/".format(master)

conf = SparkConf().setAppName(APP_NAME).setMaster('spark://{}:7077'.format(master))
sc = SparkContext(conf)
ssc = StreamingContext(sc, STREAMING_INTERVAL)
ssc.checkpoint('/tmp/ccc')

lines = ssc.textFileStream(dataFilePathOnHdfs)



res2_2 = lines.map(lambda line : line.split(","))				\
			  .filter(lambda line : line[6] == originAirport)		\ 	# 2nd argument: 'SRQ', 'CMH', 'JFK', 'SEA', or 'BOS'
			  .map(lambda line : (line[7], float(line[12])))	\	# (Carrier, Departure Delay)
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
print res2_2.take(10)
print "Gracefully stopping Spark Streaming Application"
ssc.stop(stopSparkContext = True, stopGracefully = True)
print "Application stoppped"