# Call: spark-submit --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 graphFramesGettingStarted.py
# I can't get rid of these "INFO"s
# script can also be run through pyspark line by line

# First you should synchronize the .jar file with your environment, thus
# the jar file inside
# /usr/local/Cellar/apache-spark/3.1.1/libexec/jars/graphframes-0.8.1-spark3.0-s_2.12.jar
# should be compatible with the versions installed/used in your computer.
# The version this scala script is run is shown below:
# Spark Version: 3.1.1
# Scala version: 2.12.10
# graphframes version: 0.8.1
# jar version: graphframes-0.8.1-spark3.0-s_2.12.jar
#
# After these arrangements scala does not give "graphframes/methods/etc. not found "
# errors anymore.

# https://www.davidbaldin.com/hello-graphframe-in-py-spark/

from pyspark.sql import SparkSession
from graphframes import GraphFrame
from pyspark.sql import SQLContext
from pyspark import SparkContext

spark = SparkContext()
sqlContext = SQLContext(spark)
# spark.sparkContext.setLogLevel('ERROR')

# Vertex DataFrame
v = sqlContext.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)
], ["id", "name", "age"])
# Edge DataFrame
e = sqlContext.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
], ["src", "dst", "relationship"])
# Create a GraphFrame
g = GraphFrame(v, e)

g.vertices.show()

g.edges.show()

# Get a DataFrame with columns "id" and "inDegree" (in-degree)
vertexInDegrees = g.inDegrees

# Find the youngest user's age in the graph.
# This queries the vertex DataFrame.
g.vertices.groupBy().min("age").show()

# Count the number of "follows" in the graph.
# This queries the edge DataFrame.
numFollows = g.edges.filter("relationship = 'follow'").count()
