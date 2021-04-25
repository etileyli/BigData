# called at script's directory:
# spark-submit \
#     --packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 \
#       create_transport_graph.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# // tag::imports[]
from pyspark.sql.types import *
from graphframes import *
# // end::imports[]


# // tag::load-graph-frame[]
def create_transport_graph():
    node_fields = [
        StructField("id", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("population", IntegerType(), True)
    ]
    nodes = spark.read.csv("TransportGraph/data/transport-nodes.csv", header=True, schema=StructType(node_fields))
    rels = spark.read.csv("TransportGraph/data/transport-relationships.csv", header=True)
    reversed_rels = (rels.withColumn("newSrc", rels.dst)
                     .withColumn("newDst", rels.src)
                     .drop("dst", "src")
                     .withColumnRenamed("newSrc", "src")
                     .withColumnRenamed("newDst", "dst")
                     .select("src", "dst", "relationship", "cost"))

    relationships = rels.union(reversed_rels)
    return GraphFrame(nodes, relationships)
    # return 0
# // end::load-graph-frame[]

g = create_transport_graph()

(g.vertices
     .filter("population > 100000 and population < 300000")
     .sort("population")
     .show())

from_expr = "id='Den Haag'"
to_expr = "population > 100000 and population < 300000 and id <> 'Den Haag'"
result = g.bfs(from_expr, to_expr)

print(result.columns)

columns = [column for column in result.columns if not column.startswith("e")]
result.select(columns).show()
