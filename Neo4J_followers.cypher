WITH "https://raw.githubusercontent.com/etileyli/BigData/main/" AS base
WITH base + "followersSmallSetNoWhiteSpace_252.csv" AS uri
LOAD CSV WITH HEADERS FROM uri AS row
MERGE ({ProfileID:row.ProfileID})
MERGE ({ProfileID2:row.ProfileID2})
MERGE ({DateFavorited:row.DateFavorited})

MATCH (n:column01)
RETURN n AS nodes, count(n) as cnt
UNION ALL
MATCH (n:column02)
RETURN n AS nodes, count(n) as cnt
