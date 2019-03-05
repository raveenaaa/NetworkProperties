from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as func
from pyspark.sql.functions import explode
from graphframes import *
import networkx as nx

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def closeness(g):
	
	# Get list of vertices. We'll generate all the shortest paths at
	# once using this list.
	vertices = [row.id for row in g.vertices.collect()]
	
	# first get all the path lengths.
	# Break up the map and group by ID for summing
	# Sum by ID
	# Get the inverses and generate desired dataframe.
    # YOUR CODE HERE
	path_lengths = g.shortestPaths(landmarks = vertices).select("id", explode("distances"))
	distances = path_lengths.groupBy("id").agg(func.sum("value").alias('closeness'))
	centrality = distances.withColumn('closeness', 1.0/distances['closeness'])
	return centrality	

print("Reading in graph for problem 2.")
graph = sc.parallelize([('A','B'),('A','C'),('A','D'),
	('B','A'),('B','C'),('B','D'),('B','E'),
	('C','A'),('C','B'),('C','D'),('C','F'),('C','H'),
	('D','A'),('D','B'),('D','C'),('D','E'),('D','F'),('D','G'),
	('E','B'),('E','D'),('E','F'),('E','G'),
	('F','C'),('F','D'),('F','E'),('F','G'),('F','H'),
	('G','D'),('G','E'),('G','F'),
	('H','C'),('H','F'),('H','I'),
	('I','H'),('I','J'),
	('J','I')])
	
e = sqlContext.createDataFrame(graph,['src','dst'])
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()
print("Generating GraphFrame.")
g = GraphFrame(v,e)

print("Calculating closeness.")
closeness_df = closeness(g).sort('closeness',ascending=False)
closeness_df.show()
closeness_df.toPandas().to_csv('centrality' + ".csv")