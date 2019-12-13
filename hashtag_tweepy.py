from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

# Create a local StreamingContext with two working thread and batch interval of 1 second
with open('/Users/drake/Desktop/index.json', 'r') as f:
    d = json.loads(f.read())



def sp1(x):
	ID = x.split(" ")[0]
	word = x.split(" ")[1:]
	score = d.get(word)

	return rt


def getSUm(x):
	print (type(x))
	counter = 0
	for num in x:
		print ("***" + num +"***")
		counter += int(num[1])
	return str(counter)
sc.stop()
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 10267)
#pair = lines.flatMap(sp1)
ssc.checkpoint("file:///tmp/spark")
#w = lines.reduceByKeyAndWindow(_+_,_-_,Seconds(3s),seconds(2))
#w = lines.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 3, 2)
w = lines.window(60,5)
pair = w.map(lambda x : (x.split(" ")[0],x.split(" ")[1:]))
score = pair.flatMapValues(lambda x:x).mapValues(lambda x: int(d.get(x,0))).reduceByKey(lambda x,y: x+y)
total = score.map(lambda x: (x[1],1)).reduce(lambda x,y: (x[0]+y[0], x[1] + y[1])).map(lambda x: x[0]/x[1])
#total = score.map(lambda x: x[-1]).reduceByWindow(lambda x,y:x+y,lambda x,y: x-y, 60,5)
#total = score.map(lambda x: x[-1]).foreachRDD( lambda rdd: rdd.toDF().sum( desc("count") )
#total = score.reduceByKeyAndWindow(lambda x, y: x + y, None, 60, 5).transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))
c = w.count()
#pair = lines.map(lambda x : (x.split(" ")[0],x.split(" ")[1:]))
#word = pair.flatMapValues(lambda x:x).mapValues(lambda x: int(d.get(x,0))).reduceByKey(lambda x,y: x+y)

#c.pprint()
score.pprint()
total.pprint()

#ID = lines.flatMap(lambda x: x.split(" ")[0] + x)
#tmp = pair.reduceByKey(lambda x,y: x+y)
# Count each word in each batch
#pairs = words.map(lambda word: (word, 1))
#pairs = words.map(lambda word: (word, word + d.get(word,0)))
#scoreCounts = pair.reduceByKey(lambda x, y: y)
# Print the first ten elements of each RDD generated in this DStream to the console
#w.print()
#pair.pprint()
#wordCounts.pprint()
#tmp = words.flatMap(lambda : )
#ID.pprint()
ssc.start()             # Start the computation
#ssc.awaitTermination()  # Wait for the computation to terminate
