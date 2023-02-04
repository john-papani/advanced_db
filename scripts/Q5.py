from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import Row
import time
import os
import sys

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", True)
spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize",10000)
print("spark session created")
print("================================================")
print()
print()
df = spark.read.parquet("hdfs://master:9000/files/taxi_trips.parquet")

start = time.time()

df5 = df.withColumn('day', date_format(df['tpep_pickup_datetime'], "yyyy/MM/dd"))\
        .withColumn('percent_tip', when(df.fare_amount == '0.0', 0.0)\
        .otherwise(round(df.tip_amount/df.fare_amount*100,2)))\
        .withColumn('month',month(df.tpep_pickup_datetime))\
        .select('day', 'month', 'percent_tip').cache()

#find and keep only the max percent_tip of each day
df5 = df5.withColumn('row', row_number()\
         .over(Window.partitionBy('day')\
         .orderBy(col('percent_tip').desc())))\
         .filter(col("row") == 1)\
         .drop('row')

#find the top 5 days of each month 
dfQ5 = df5.withColumn('row', row_number()\
          .over(Window.partitionBy('month')\
          .orderBy(col('percent_tip').desc())))\
          .filter(col("row") < 6)\
          .drop('row')\
          .sort(col('month').asc())\
          .limit(30)

dfQ5.write.parquet("hdfs://master:9000/outputs/Q5.parquet")

end = time.time()

df5.unpersist()

#save output time as .txt file
with open("Queries_exec_times.txt", "a") as f:
        f.write("Q5: {} sec\n".format(end-start))