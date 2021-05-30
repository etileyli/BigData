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

println(s"Total Number of Names: ${followersGraph.vertices.count()}")
println(s"Total Number of Connections in Graph: ${followersGraph.edges.count()}")
