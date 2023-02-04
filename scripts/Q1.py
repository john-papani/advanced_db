from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys
import time

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print('')
print("spark session created")

df = spark.read.parquet("hdfs://master:9000/files/taxi_trips.parquet")

df2 = spark.read.parquet("hdfs://master:9000/files/zone_lookups.parquet")

start = time.time()

#find Battery's Park location id from df2
locationid = df2.select(df2["LocationID"]).filter(df2.Zone == "Battery Park")
location_id = locationid.collect()[0][0]

dfQ1 = df.filter((df['tpep_pickup_datetime'].startswith("2022-03"))
& (df['DOLocationID']== location_id)).orderBy(col("Tip_amount").desc()).limit(1)

dfQ1.write.parquet("hdfs://master:9000/outputs/Q1.parquet")

end = time.time()

#save output time as .txt file
with open("Queries_exec_times.txt", "a") as f:
        f.write("Q1: {} sec\n".format(end-start))