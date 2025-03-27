from pyspark import SparkContext, SparkConf

# Initialize Spark context
conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)

# Load a text file from HDFS
text_file = sc.textFile("hdfs://hadoop-master-node:9000/testing_directory/testing.txt")

# Count the occurrences of each word
word_counts = text_file.flatMap(lambda line: line.split(" ")) \
                       .map(lambda word: (word, 1)) \
                       .reduceByKey(lambda a, b: a + b)

# Save the results to an output directory in HDFS
word_counts.saveAsTextFile("hdfs://hadoop-master-node:9000/output/word_count_result9")

# Stop the Spark context
sc.stop()
