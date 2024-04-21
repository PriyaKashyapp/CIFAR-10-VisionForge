import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

from functools import reduce
from pyspark.sql.functions import col, lit, when
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
import graphframes
from graphframes import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
        .appName("graphframes") \
        .getOrCreate()

    sqlContext = SQLContext(spark)
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    rideshare_data = spark.read.option("delimiter", ",").option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    taxi_zone_lookup = spark.read.option("delimiter",",").option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")

    
    #OPTIONAL Task 8 (20 points): Graph Processing
    
    #Task 8(1): Define StructTypes for Vertices and Edges
    vertexSchema = StructType()
    edgeSchema = StructType()

    #Task 8(2): Construct DataFrames for Edges and Vertices
    edgesDF = rideshare_data.withColumn('src',col('pickup_location')).withColumn('dst',col('dropoff_location'))
    edgesDF = edgesDF.select('src','dst')
    verticesDF = taxi_zone_lookup
    verticesDF = verticesDF.withColumnRenamed('LocationID','id')
    # showing 10 rows from the vertices and edges tables
    edgesDF.show(10)
    # create a graph using the vertices and edges
    verticesDF.show(10,truncate=False)


    #Task 8(3): Create a Graph and Sample it
    graph = GraphFrame(verticesDF, edgesDF)
    # Print 10 samples of the graph DataFrame
    graph_df = graph.find("(src)-[edge]->(dst)").limit(10).select("src", "edge", "dst")
    graph_df.show(truncate=False)

    #Task 8(4): Count Connected Vertices with the Same Borough and Service Zone
    # Count connected vertices with the same Borough and service_zone
    borough_zone_count_df = graph.edges.join(verticesDF.alias("src"), graph.edges["src"] == verticesDF["id"]) \
                           .join(verticesDF.alias("dst"), graph.edges["dst"] == verticesDF["id"]) \
                           .filter("src.Borough = dst.Borough AND src.service_zone = dst.service_zone") \
                           .groupBy("src.id", "dst.id", "src.Borough", "src.service_zone").count()
 
    counts_df = borough_zone_count_df.drop('count')
    # Show the samples and count
    counts_df.show(10)

    #Task 8(5): Perform PageRank
    # Perform PageRank with resetProbability set to 0.17 and tol set to 0.01
    page_rank = graph.pageRank(resetProbability=0.17, tol=0.01)
    # Get the DataFrame of vertices with PageRank values
    pagerank_df = page_rank.vertices
    # Sort vertices by descending PageRank value
    pagerank_df = pagerank_df.orderBy(pagerank_df["pagerank"].desc())
    # Show the top 5 samples of results
    pagerank_df.show(5)

    
# Final Solution: 
# +---+--------+-------------------+------------+------------------+
# | id| Borough|               Zone|service_zone|          pagerank|
# +---+--------+-------------------+------------+------------------+
# |265| Unknown|                 NA|         N/A|11.105433344108016|
# |  1|     EWR|     Newark Airport|         EWR| 5.471845424920979|
# |132|  Queens|        JFK Airport|    Airports| 4.551132572067707|
# |138|  Queens|  LaGuardia Airport|    Airports|3.5683223416560734|
# | 61|Brooklyn|Crown Heights North|   Boro Zone|2.6763973653417987|
# +---+--------+-------------------+------------+------------------+

    
    spark.stop()




