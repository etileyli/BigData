// WITH "https://raw.githubusercontent.com/etileyli/BigData/main/" AS base
// WITH "file:///Users/emre/github/BigData/" AS base

WITH "file:///followersSmallSetNoWhiteSpace_252.csv" AS uri
LOAD CSV WITH HEADERS FROM uri AS row
MERGE (nd1: Node {id:row.ProfileID})
MERGE (nd2: Node {id:row.ProfileID2})
MERGE (df:dateFav {DateFavorited:row.DateFavorited})

// WITH "https://raw.githubusercontent.com/etileyli/BigData/main/" AS base
// WITH "/Users/emre/github/BigData/" AS base
WITH "file:///followersSmallSetNoWhiteSpace_252.csv" AS uri
LOAD CSV WITH HEADERS FROM uri AS row
MATCH (origin:Node {id: row.ProfileID})
MATCH (destination:Node {id: row.ProfileID2})
MERGE (origin)-[:FOLLOWS {DateFavorited: row.DateFavorited}]->(destination);

MATCH (n:Node)
RETURN n AS nodes, count(n) as cnt
UNION ALL
MATCH (n:Node)
RETURN n AS nodes, count(n) as cnt

// PAGE RANK
CALL gds.pageRank.stream({
      nodeProjection: "Node",
      relationshipProjection: "FOLLOWS",
      maxIterations: 20,
      dampingFactor: 0.85
    })
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).id AS page, score ORDER BY score DESC;
