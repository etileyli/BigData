
// TEST BFS WITH SMALLER FILE
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions.expr

val namesTempp = spark.read.option("header","true").csv("data/followersSmallNoWhiteSpaceTemp.csv")
namesTempp.count()

val nameVerticesTemp01 = namesTempp.select("ProfileID").withColumnRenamed("ProfileID", "id").distinct() // (select distinct applies to the ENTIRE row, not just a field.)
val nameVerticesTemp02 = namesTempp.select("ProfileID2").withColumnRenamed("ProfileID2", "id").distinct()

val nameVerticesTempTemp = nameVerticesTemp01.union(nameVerticesTemp02)
val nameVerticesTempp = nameVerticesTempTemp.distinct()
nameVerticesTempp.count()

val timeStampEdgesTemp = namesTempp.withColumnRenamed("ProfileID", "src").withColumnRenamed("ProfileID2", "dst")

val followersGraphTemp = GraphFrame(nameVerticesTempp, timeStampEdgesTemp)

println(s"Total Number of Names: ${followersGraphTemp.vertices.count()}")
println(s"Total Number of Connections in Graph: ${followersGraphTemp.edges.count()}")

followersGraphTemp.vertices.show()
followersGraphTemp.edges.show()

val sourceNode = "emre"
val destNode = "gizem"
followersGraphTemp.bfs.fromExpr("id = 'emre'").toExpr("id = 'gizem'").maxPathLength(2).run().show(10)
// SUCCESFUL SEARCHES
// followersGraphTemp.bfs.fromExpr("id = 'A43686FA05B9706827AD2D71AC062D22BC20C352'").toExpr("id = 'F38C2741C973F0A909B346862661087AA9204369'").maxPathLength(2).run().show(10)
// followersGraphTemp.bfs.fromExpr("id = 'B570C0733DDB00A3FEECC4E846EFAA5799454444'").toExpr("id = '3416A6F78271208D31DFAAD0B37F937B18A295E3'").maxPathLength(2).run().show(10)
// followersGraphTemp.bfs.fromExpr("id = 'DD387DC815CC8BD9CE423D96CD5C9F7A5D83E155'").toExpr("id = 'D5E0FF018390A9061975F25CB96E574288A3BD69'").maxPathLength(3).run().show(10)



















//.
