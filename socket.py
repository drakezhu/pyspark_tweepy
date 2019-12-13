
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

# Create a local StreamingContext with two working thread and batch interval of 1 second
with open('/Users/drake/Desktop/index.json', 'r') as f:
    d = json.loads(f.read())
sc.stop()
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 10284)
words = lines.flatMap(lambda line: line.split(" "))
# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
#pairs = words.map(lambda word: (word, d.get(word,0)))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
