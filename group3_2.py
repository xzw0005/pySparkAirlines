import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext

APP_NAME = "Group3_2_Tom'sBestChoice"
STREAMING_INTERVAL = 1

master = str(sys.argv[1]) # 'ec2-xx-xx-xx-xx.compute-1.amazonaws.com'
X = str(sys.argv[2])
Y = str(sys.argv[3])
Z = str(sys.argv[4])
year = str(sys.argv[5])
month = str(sys.argv[6])
day = str(sys.argv[7]) 

dataFilePathOnHdfs = "hdfs://{}/btsdata/aviation/ontime/".format(master)

conf = SparkConf().setAppName(APP_NAME).setMaster('spark://{}:7077'.format(master))
sc = SparkContext(conf)
ssc = StreamingContext(sc, STREAMING_INTERVAL)
ssc.checkpoint('/tmp/ccc')

lines = ssc.textFileStream(dataFilePathOnHdfs)
XYlines = lines.map(lambda line : line.split(","))		\
				.filter(lambda line : line[6] == X and line[7] == Y)	\
				.filter(lambda line : int(line[0]) == year and int(line[1]) == month and int(line[2]) == day) \
				.filter(lambda line : int(line[8]) < 1200) \
				.map(lambda line : (line[6], line[7], line[4], line[5], line[8], line[11]))  \ #(X, Y, Carrier, Flight#, DepartTime, ArrivalDelay)
				.map(lambda (X, Y, Carrier, FlightNo, DepartTime, ArrivalDelay) : ((X, Y, Carrier, FlightNo, DepartTime), ArrivalDelay)) \
				.sortByKey('ascending')

YZlines = lines.map(lambda line : line.split(","))		\
				.filter(lambda line : line[6] == Y and line[7] == Z)	\
				.filter(lambda line : int(line[0]) == year and int(line[1]) == month and int(line[2]) == day) \
				.filter(lambda line : int(line[8]) >= 1200)	\
				.map(lambda line : (line[6], line[7], line[4], line[5], line[8], line[11]))  \ #(Y, Z, Carrier, Flight#, DepartTime, ArrivalDelay)
				.map(lambda (Y, Z, Carrier, FlightNo, DepartTime, ArrivalDelay) : ((Y, Z, Carrier, FlightNo, DepartTime), ArrivalDelay)) \
				.sortByKey('ascending')

res3_3 = XYlines.join(YZlines).take(1)


ssc.start()
while true:
	if ssc.awaitTerminationOrTimeout(10):
		break
	else:
		pass
print res2_3.take(10)
print "Gracefully stopping Spark Streaming Application"
ssc.stop(stopSparkContext = True, stopGracefully = True)
print "Application stoppped"