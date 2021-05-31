import org.graphframes.GraphFrame

// Load data
// val names = spark.read.option("header","true").csv("data/followers")
val names = spark.read.option("header","true").csv("data/followersSmallNoWhiteSpace.csv")
//val names = spark.read.option("header","true").csv("data/followersSmall.csv")

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

// MOTIF FINDING
//val motifs = followersGraph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")

//spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

//motifs.selectExpr("*",
//    "to_timestamp(ab.`Start Date`, 'yyyy/MM/dd HH:mm:ss.SSSSSSSSS') as abStart",
//    "to_timestamp(bc.`Start Date`, 'yyyy/MM/dd HH:mm:ss.SSSSSSSSS') as bcStart",
//    "to_timestamp(ca.`Start Date`, 'yyyy/MM/dd HH:mm:ss.SSSSSSSSS') as caStart")
//    .where("ca.`Bike #` = bc.`Bike #`")

// GRAPH ALGORITHMS

// PageRank (It takes ~1 hour to finish)
import org.apache.spark.sql.functions.desc
//val ranks = followersGraph.pageRank.resetProbability(0.15).maxIter(10).run()
//ranks.vertices.orderBy(desc("pagerank")).select("id", "pagerank").show(10)

import org.apache.spark.sql.functions.expr


// Breadth-First-Search
import org.graphframes.GraphFrame

followersGraph.bfs.fromExpr("id = '601BB03C9BF3B57BB6E9DCA48218386BE08FA897'").toExpr("id = 'A77BE9FDC675901E5D01F116205253C8179C187C'").maxPathLength(2).run().show(10)
// stationGraph.bfs.fromExpr("id = 'Townsend at 7th'").toExpr("id = 'Spear at Folsom'").maxPathLength(10).run().take(10).foreach(println)

// TEST BFS WITH SMALLER FILE
// val namesTempp = spark.read.option("header","true").csv("data/followersSmallNoWhiteSpaceTemp.csv")
// namesTempp.count()
//
// val nameVerticesTemp01 = namesTempp.select("ProfileID").withColumnRenamed("ProfileID", "id").distinct() // (select distinct applies to the ENTIRE row, not just a field.)
// val nameVerticesTemp02 = namesTempp.select("ProfileID2").withColumnRenamed("ProfileID2", "id").distinct()
//
// val nameVerticesTempTemp = nameVerticesTemp01.union(nameVerticesTemp02)
// val nameVerticesTempp = nameVerticesTempTemp.distinct()
// nameVerticesTempp.count()
//
// val timeStampEdgesTemp = namesTempp.withColumnRenamed("ProfileID", "src").withColumnRenamed("ProfileID2", "dst")
//
// val followersGraphTemp = GraphFrame(nameVerticesTempp, timeStampEdgesTemp)
//
// println(s"Total Number of Names: ${followersGraphTemp.vertices.count()}")
// println(s"Total Number of Connections in Graph: ${followersGraphTemp.edges.count()}")
//
// followersGraphTemp.vertices.show()
// followersGraphTemp.edges.show()
//
// import org.apache.spark.sql.functions.expr
// val sourceNode = "emre"
// val destNode = "gizem"
// followersGraphTemp.bfs.fromExpr("id = 'emre'").toExpr("id = 'gizem'").maxPathLength(2).run().show(10)
// followersGraphTemp.bfs.fromExpr("id = sourceNode").toExpr("id = destNode").maxPathLength(2).run().show(10)  // Fails to run
// followersGraphTemp.bfs.fromExpr("id = 'A43686FA05B9706827AD2D71AC062D22BC20C352'").toExpr("id = 'F38C2741C973F0A909B346862661087AA9204369'").maxPathLength(2).run().show(10)
// followersGraphTemp.bfs.fromExpr("id = 'B570C0733DDB00A3FEECC4E846EFAA5799454444'").toExpr("id = '3416A6F78271208D31DFAAD0B37F937B18A295E3'").maxPathLength(2).run().show(10)



















//.
