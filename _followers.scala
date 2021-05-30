// Load data
val names = spark.read.option("header","true").csv("data/followers")

// Count of entries in file
names.distinct().count()
// Result = (Long = 16605113) (Also line count)

// Get distinct ProfileIDs
val nameVertices = names.select("ProfileID").distinct()

// Count of distinct ProfileIDs (Vertices)
nameVertices.count()
// Result = (Long = 2016596)
