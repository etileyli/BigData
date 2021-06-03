LOAD CSV FROM "followersSmallNoWhiteSpace_2_6_MB.csv" WITH HEADER DELIMITER "," AS row
MERGE (nd1: Node {id:row.ProfileID})
MERGE (nd2: Node {id:row.ProfileID2})
MERGE (df:dateFav {DateFavorited:row.DateFavorited})

LOAD CSV FROM "followersSmallNoWhiteSpace_2_6_MB.csv" WITH HEADER DELIMITER "," AS row
MATCH (origin:Node {id: row.ProfileID})
MATCH (destination:Node {id: row.ProfileID2})
MERGE (origin)-[:FOLLOWS {DateFavorited: row.DateFavorited}]->(destination);

MATCH (n:Node)
RETURN n AS nodes, count(n) as cnt
UNION ALL
MATCH (n:Node)
RETURN n AS nodes, count(n) as cnt

CALL nxalg.pagerank()
YIELD node, rank
RETURN node, rank ORDER BY rank DESC


// delete all nodes and Edges
// MATCH (n)
// DETACH DELETE n
