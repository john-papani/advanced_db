from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os
import sys
import time

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
print("spark session created")
print("================================================")
print()
print()

#Read parquet and csv files into spark dataframes
df_trips = spark.read.parquet("hdfs://master:9000/files/")
df_zones = spark.read.option("header",True).csv("hdfs://master:9000/taxi+_zone_lookup.csv")

#Create two RDD from the above dataframes
rdd_trips = df_trips.rdd
rdd_zones = df_zones.rdd

#Show the two dataframes
print("Taxi_Trips Dataframe: \n")
df_trips.printSchema()  
df_trips.sort(col("tpep_pickup_datetime").asc()).show(5)

print("Zone&Lookups Dataframe: \n")
df_zones.printSchema()  
df_zones.show(5)

#Print the number of partitions and the first element of each RDD
print("\nRDD_trips Number of Partitions: "+str(rdd_trips.getNumPartitions()))
print("\nFirst element of RDD_trips: \n"+str(rdd_trips.first()))
print("\nRDD_zones Number of Partitions: "+str(rdd_zones.getNumPartitions()))
print("\nFirst element of RDD_zones: \n"+str(rdd_zones.first()))

# clean dataframe, only 01-22 --- 06-22
df_2022 = df_trips.filter(df_trips['tpep_pickup_datetime'].startswith("2022") & df_trips['tpep_dropoff_datetime'].startswith("2022"))
df = df_2022.filter((df_2022['tpep_pickup_datetime'] < '2022-07-01 00:00:00') & (df_2022['tpep_dropoff_datetime'] < '2022-07-01 23:59:59'))

print("\nCleaned Taxi_Trips Dataframe: \n")
df.sort(col("tpep_pickup_datetime").asc()).show(5)

#Save the two dataframes as parquet files
df.write.format("parquet").save("hdfs://master:9000/files/taxi_trips.parquet")
df_zones.write.format("parquet").save("hdfs://master:9000/files/zone_lookups.parquet")