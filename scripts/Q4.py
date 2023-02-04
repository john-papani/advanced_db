from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import Row
import pyspark
import os
import sys
import time

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
print("spark session created")
print("================================================")
print()
print()

df = spark.read.parquet("hdfs://master:9000/files/taxi_trips.parquet")

start = time.time()

df4 = df.withColumn('day_of_week',  date_format(df['tpep_pickup_datetime'], "EEEE"))\
        .withColumn('hour', hour(df['tpep_pickup_datetime']))\
        .select('day_of_week', 'hour', 'Passenger_count')

dfQ4 =df4.withColumn('row', row_number()\
          .over(Window.partitionBy('day_of_week')\
          .orderBy(col('Passenger_count').desc())))\
          .dropDuplicates()\
          .filter(col("row") < 4)\
          .drop('row')\
          .limit(21)

dfQ4.write.parquet("hdfs://master:9000/outputs/Q4.parquet")
end = time.time()

#save output time as .txt file
with open("Queries_exec_times.txt", "a") as f:
        f.write("Q4: {} sec\n".format(end-start))                                                