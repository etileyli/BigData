// call: spark-shell --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12

import org.graphframes.GraphFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.{desc, asc}

// Load data
// val names = spark.read.option("header","true").csv("data/followers")
val names = spark.read.option("header","true").csv("data/followersSmallNoWhiteSpace.csv")
// val names = spark.read.option("header","true").csv("data/followersSmallNoWhiteSpace_271302.csv")
// val names = spark.read.option("header","true").csv("data/followersSmallNoWhiteSpace_2_6_MB.csv")
// val names = spark.read.option("header","true").csv("data/followersSmall.csv")

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
val ranks = followersGraph.pageRank.resetProbability(0.15).maxIter(10).run()
ranks.vertices.orderBy(desc("pagerank")).select("id", "pagerank").show(10)

// Breadth-First-Search
// SUCCESSFUL RUNS
// via 864E7B4F47CF11E6A25372AAB334021AAE81A445
followersGraph.bfs.fromExpr("id = '601BB03C9BF3B57BB6E9DCA48218386BE08FA897'").toExpr("id = 'A77BE9FDC675901E5D01F116205253C8179C187C'").maxPathLength(2).run().show(10)

followersGraph.bfs.fromExpr("id = '601BB03C9BF3B57BB6E9DCA48218386BE08FA897'").toExpr("id = 'A77BE9FDC675901E5D01F116205253C8179C187C'").maxPathLength(3).run().show(10)

// VIA C39F94D9576B1548FCCE1029FA68882F31888ABB and 9D36FF9F3473B1D85FA75155AAE4439A7DEEE526
// followersGraph.bfs.fromExpr("id = 'DD387DC815CC8BD9CE423D96CD5C9F7A5D83E155'").toExpr("id = 'D5E0FF018390A9061975F25CB96E574288A3BD69'").maxPathLength(3).run().show(10)
followersGraph.bfs.fromExpr("id = '7140746EDF056F3DD1585956EA8C753FB0242824'").toExpr("id = '9658E06075B2B3998772B5D31F472DA6553E167F'").maxPathLength(4).run().show(10)
// followersGraph.bfs.fromExpr("id = 'DD387DC815CC8BD9CE423D96CD5C9F7A5D83E155'").toExpr("id = 'D5E0FF018390A9061975F25CB96E574288A3BD69'").maxPathLength(3).run().show(10)




//.
