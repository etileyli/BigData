// Load data
val names = spark.read.option("header","true").csv("data/followers")

// Count of entries in file
names.distinct().count()
// Result = (Long = 16605113) (Also line count)

// Get distinct ProfileIDs
val nameVertices01 = names.select("ProfileID").distinct() // (select distinct applies to the ENTIRE row, not just a field.)

// Count of distinct ProfileIDs (Vertices)
nameVertices01.count()
// Result = (Long = 2016596)

// Get distinct ProfileIDs
val nameVertices02 = names.select("ProfileID2").distinct()

// Count of distinct ProfileID2s (Vertices)
nameVertices02.count()
// Result = (Long = 1719956)

val nameVerticesTemp = nameVertices01.union(nameVertices02)

// nameVerticesTemp.count()

// Aggregate Name Vertices
val nameVertices = nameVerticesTemp.distinct()
nameVertices.count()
// Result = (Long = 3071487 )
