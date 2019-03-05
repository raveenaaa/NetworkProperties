# Network Properties in Spark GraphFrames
Implementation of various network properties using pySpark and GraphFrames. The various output Graphs are present in the `Graphs` folder and the various csv output files are present in the `CSV` folder. The `stanford_graphs` folder contains the various input graphs.

Run the following commands on any new terminal before running any code:
* `export SPARK_HOME=/usr/local/spark-1.6.2-bin-hadoop2.6`
* `export PYSPARK_PYTHON=/usr/bin/python2`
* `export PYSPARK_DRIVER_PYTHON=/usr/bin/python2`

#### Degree: ####  
The degree disstribution is the measure of the frequency of nodes that have a certain degree. The various graphs have been plotted using matplotlib. 

Run the file using: 
  * `$SPARK_HOME/bin/pyspark --packages graphframes:graphframes:0.1.0-spark1.6 degree.py ./stanford_graphs/amazon.graph.large large`
    OR
  * `$SPARK_HOME/bin/pyspark --packages graphframes:graphframes:0.1.0-spark1.6 degree.py ./stanford_graphs/amazon.graph.small`

#### Centrality ####
Centrality is a is a way to determine nodes that are important based on the structure of the graph. Here we have used *Closeness Centrality* which is a measure of the distance of a node to all other nodes. The output of the file is in centrality.csv

Run the file using:
* `$SPARK_HOME/bin/pyspark --packages graphframes:graphframes:0.1.0-spark1.6 centrality.py`

#### Articulation ####
Articulation points are vertices in the graph that when removed, create more components than there were originally.

Run the file using:
* `$SPARK_HOME/bin/pyspark --packages graphframes:graphframes:0.1.0-spark1.6 articulation.py 9_11_edgelist.txt`

