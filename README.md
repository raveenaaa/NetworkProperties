# Network Properties in Spark GraphFrames
Implementation of various network properties using pySpark and GraphFrames. The various output Graphs are present in the `Graphs` folder and the various csv output files are present in the `CSV` folder. The `stanford_graphs` folder contains the various input graphs.

Run the following commands on any new terminal before running any code:
* `export SPARK_HOME=/usr/local/spark-1.6.2-bin-hadoop2.6`
* `export PYSPARK_PYTHON=/usr/bin/python2`
* `export PYSPARK_DRIVER_PYTHON=/usr/bin/python2`

#### Degree: ####  
The various graphs have been plotted using matplotlib. The plot function gives us the plot of the various graphs in the degree.py file. The various csv files have been named according to their corresponding data files. Run the file using 
  * `$SPARK_HOME/bin/pyspark --packages graphframes:graphframes:0.1.0-spark1.6 degree.py ./stanford_graphs/amazon.graph.large large`
  * OR
  * `$SPARK_HOME/bin/pyspark --packages graphframes:graphframes:0.1.0-spark1.6 degree.py ./stanford_graphs/amazon.graph.small`

2. centrality
->The output of the file is in centrality.csv

3. articulation
->The output of the file has been saved to articulation.csv

The answers to various questions of the section has been saved to Answers.pdf
