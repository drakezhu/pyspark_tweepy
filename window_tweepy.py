from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

# Create a local StreamingContext with two working thread and batch interval of 1 second
with open('/Users/drake/Desktop/index.json', 'r') as f:
    d = json.loads(f.read())



sc.stop()
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 10238)
#pair = lines.flatMap(sp1)

#w = lines.reduceByKeyAndWindow(_+_,_-_,Seconds(3s),seconds(2))
#w = lines.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 3, 2)
w = lines.window(60,5)
c = w.count()
#pair = lines.map(lambda x : (x.split(" ")[0],x.split(" ")[1:]))
#word = pair.flatMapValues(lambda x:x).mapValues(lambda x: int(d.get(x,0))).reduceByKey(lambda x,y: x+y)

c.pprint()
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
