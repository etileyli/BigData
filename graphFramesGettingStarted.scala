// call: spark-shell --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12
// script can be run through spark-shell line by line

// First you should synchronize the .jar file with your environment, thus
// the jar file inside
// /usr/local/Cellar/apache-spark/3.1.1/libexec/jars/graphframes-0.8.1-spark3.0-s_2.12.jar
// should be compatible with the versions installed/used in your computer.
// The version this scala script is run is shown below:
// Spark Version: 3.1.1
// Scala version: 2.12.10
// graphframes version: 0.8.1
// jar version: graphframes-0.8.1-spark3.0-s_2.12.jar
//
// After these arrangements scala does not give "graphframes/methods/etc. not found "
// errors anymore.

import org.graphframes.GraphFrame
import org.apache.spark.sql.DataFrame

// Vertex DataFrame
val v = spark.sqlContext.createDataFrame(List(
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)
)).toDF("id", "name", "age")
// Edge DataFrame
val e = spark.sqlContext.createDataFrame(List(
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
)).toDF("src", "dst", "relationship")
// Create a GraphFrame
val g = GraphFrame(v, e)

g.vertices.show()
// +--+-------+---+
// |id|   name|age|
// +--+-------+---+
// | a|  Alice| 34|
// | b|    Bob| 36|
// | c|Charlie| 30|
// | d|  David| 29|
// | e| Esther| 32|
// | f|  Fanny| 36|
// | g|  Gabby| 60|
// +--+-------+---+

g.edges.show()
// +---+---+------------+
// |src|dst|relationship|
// +---+---+------------+
// |  a|  b|      friend|
// |  b|  c|      follow|
// |  c|  b|      follow|
// |  f|  c|      follow|
// |  e|  f|      follow|
// |  e|  d|      friend|
// |  d|  a|      friend|
// |  a|  e|      friend|
// +---+---+------------+


// Get a DataFrame with columns "id" and "inDeg" (in-degree)
val vertexInDegrees: DataFrame = g.inDegrees

// Find the youngest user's age in the graph.
// This queries the vertex DataFrame.
g.vertices.groupBy().min("age").show()

// Count the number of "follows" in the graph.
// This queries the edge DataFrame.
val numFollows = g.edges.filter("relationship = 'follow'").count()

val motifs: GraphFrame = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
