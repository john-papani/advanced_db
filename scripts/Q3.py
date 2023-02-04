from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import time
import os
import sys

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
print("spark session created")
print("================================================")
print()
print()
df = spark.read.parquet("hdfs://master:9000/files/taxi_trips.parquet")

start = time.time()

df3 = df.select("tpep_pickup_datetime","PULocationID","DOLocationID","Trip_distance","Total_amount")\
        .filter(df['PULocationID'] != df['DOLocationID'])\
        .groupBy(window("tpep_pickup_datetime", "15 days", startTime="70 hours"))\
        .agg(round(avg("Trip_distance"),2).alias("Average_Trip_Distance"), round(avg("Total_amount"),2).alias("Average_Cost"))\
        .sort(col("window").asc())


df3.write.parquet("hdfs://master:9000/outputs/Q3.parquet")
end = time.time()

with open("Queries_exec_times.txt", "a") as f:
        f.write("Q3: " + str(end-start) + " sec\n")