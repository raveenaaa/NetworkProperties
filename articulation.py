import sys
import time
import networkx as nx
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as func
from graphframes import *
from copy import deepcopy

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def articulations(g, usegraphframe=False):
	# Get the starting count of connected components
	# YOUR CODE HERE
	connected = g.connectedComponents()
	count_connected = connected.select('component').distinct().count()
	articulation = []
	vertices = [row.id for row in g.vertices.collect()]

	# Default version sparkifies the connected components process 
	# and serializes node iteration.
	if usegraphframe:
		# Get vertex list for serial iteration
		# For each vertex, generate a new graphframe missing that vertex
		# and calculate connected component count. Then append count to
		# the output
		for vertex in vertices:
			graph = g			
			graph.edges.filter((func.col('src') != vertex) and (func.col('dst') != vertex))
			graph.vertices.filter((func.col('id') != vertex))
			connected = graph.connectedComponents()
			if count_connected < connected.select('component').distinct().count():
				articulation.append([vertex, 1])
			else:
				articulation.append([vertex, 0])
        
	# Non-default version sparkifies node iteration and uses networkx 
	# for connected components count.
	else:
		edge_list = [(row.src, row.dst) for row in g.edges.collect()]
		graphNX = nx.Graph()
		graphNX.add_edges_from(edge_list)

		def generate_graph(x):
			graph = deepcopy(graphNX)		
			graph.remove_node(x.id)
			connected = nx.number_connected_components(graph)
			return [x.id, connected]

		articulation = g.vertices.select("id").map(generate_graph).map(lambda rdd: [rdd[0], 1] if count_connected < rdd[1] else [rdd[0], 0]).collect()

	result = sqlContext.createDataFrame(articulation, ['id', 'articulation'])
	return result

filename = sys.argv[1]
lines = sc.textFile(filename)

pairs = lines.map(lambda s: s.split(","))
e = sqlContext.createDataFrame(pairs,['src','dst'])
e = e.unionAll(e.selectExpr('dst as src', 'src as dst')).distinct() # Ensure undirectedness 	

# Extract all endpoints from input file and make a single column frame.
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()	

# Create graphframe from the vertices and edges.
g = GraphFrame(v,e)
# Runtime approximately 5 minutes
print("---------------------------")
print("Processing graph using Spark iteration over nodes and serial (networkx) connectedness calculations")
init = time.time()
df = articulations(g, False)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
articulations = df.filter('articulation = 1')
articulations.toPandas().to_csv("CSV/articulation.csv")
artiulations.show(truncate = False)

print("---------------------------")

# Runtime for below is more than 2 hours
print("Processing graph using serial iteration over nodes and GraphFrame connectedness calculations")
init = time.time()
df = articulations(g, True)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
articulations = df.filter('articulation = 1').show(truncate = False)

