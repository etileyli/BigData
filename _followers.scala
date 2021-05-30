import org.graphframes.GraphFrame

// Load data
// val names = spark.read.option("header","true").csv("data/followers")
val names = spark.read.option("header","true").csv("data/followersSmall.csv")

// Count of entries in file
names.distinct().count()
// Result = (Long = 16605113) (Also line count)

// Get distinct ProfileIDs
val nameVertices01 = names.select("ProfileID").withColumnRenamed("ProfileID", "id").distinct() // (select distinct applies to the ENTIRE row, not just a field.)

// Count of distinct ProfileIDs (Vertices)
nameVertices01.count()
// Result = (Long = 2016596)

// Get distinct ProfileIDs
val nameVertices02 = names.select("ProfileID2").withColumnRenamed("ProfileID2", "id").distinct()

// Count of distinct ProfileID2s (Vertices)
nameVertices02.count()
// Result = (Long = 1719956)

val nameVerticesTemp = nameVertices01.union(nameVertices02)

// nameVerticesTemp.count()

// Aggregate Name Vertices
val nameVertices = nameVerticesTemp.distinct()
nameVertices.count()
// Result = (Long = 3071487)
nameVertices.show(10)

val timeStampEdges = names.withColumnRenamed("ProfileID", "src").withColumnRenamed("ProfileID2", "dst")
timeStampEdges.count()
timeStampEdges.show(10)
// Result = (Long = 16605113)

val followersGraph = GraphFrame(nameVertices, timeStampEdges)
followersGraph.cache()

println(s"Total Number of Names: ${followersGraph.vertices.count()}")   // Total Number of Names: 617680
println(s"Total Number of Connections in Graph: ${followersGraph.edges.count()}")  // Total Number of Connections in Graph: 2313358

//
// QUERYING THE GRAPH
//
import org.apache.spark.sql.functions.{desc, asc}
followersGraph.edges.orderBy(desc("src")).show(10)
followersGraph.edges.orderBy(asc("src")).show(10)

followersGraph.edges.groupBy("src", "dst").count().orderBy(desc("src")).show(10)
followersGraph.edges.groupBy("src", "dst").count().orderBy(asc("src")).show(10)

followersGraph.edges.select("src").show(10)
followersGraph.edges.select("src").count()
followersGraph.edges.filter(($"src" rlike "864E7B4F47CF11E6A25372AAB334021AAE81A445")).show()
followersGraph.edges.filter(($"dst" rlike "864E7B4F47CF11E6A25372AAB334021AAE81A445")).show()
followersGraph.vertices.filter(($"id" rlike "864E7B4F47CF11E6A25372AAB334021AAE81A445")).show()

followersGraph.edges.filter($"src".contains("864E7B4F47CF11E6A25372AAB334021AAE81A445") || $"dst".contains("864E7B4F47CF11E6A25372AAB334021AAE81A445")).show()

val filterPar = $"src".contains("864E7B4F47CF11E6A25372AAB334021AAE81A445") || $"dst".contains("864E7B4F47CF11E6A25372AAB334021AAE81A445")
followersGraph.edges.filter(filterPar).show()

val user01Edges = followersGraph.edges.where(filterPar)
val subgraph = GraphFrame(followersGraph.vertices, user01Edges)
subgraph.vertices.show(100)
subgraph.edges.show(100)
