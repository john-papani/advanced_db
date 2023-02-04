from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime
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
#df.printSchema()
rdd = df.rdd

start = time.time()

rdd = rdd.filter(lambda x: x.PULocationID != x.DOLocationID)

#transform the data into a tuple of (date, (money, distance))
rdd = rdd.map(lambda x: (x.tpep_pickup_datetime.strftime("%Y-%m-%d"), (x.total_amount, x.trip_distance)))

def fortnight(x):
    date_str = x[0]
    cost = x[1][0]
    distance = x[1][1]
    period = 0

    if date_str < '2022-01-16':
        period = 1
    elif date_str >= '2022-01-16' and date_str < '2022-01-31':
        period = 2
    elif date_str >= '2022-01-31' and date_str < '2022-02-15':
        period = 3
    elif date_str >= '2022-02-15' and date_str < '2022-03-02':
        period = 4
    elif date_str >= '2022-03-02' and date_str < '2022-03-17':
        period = 5
    elif date_str >= '2022-03-17 ' and date_str < '2022-04-01':
        period = 6
    elif date_str >= '2022-04-01' and date_str < '2022-04-16':
        period = 7
    elif date_str >= '2022-04-16' and date_str < '2022-05-01':
        period = 8
    elif date_str >= '2022-05-01' and date_str < '2022-05-16':
        period = 9
    elif date_str >= '2022-05-16' and date_str < '2022-05-31':
        period = 10
    elif date_str >= '2022-05-31' and date_str < '2022-06-15':
        period = 11
    elif date_str >= '2022-06-15' and date_str < '2022-06-30':
        period = 12
    else: period = 13
    return (period, (cost, distance, 1))

rdd = rdd.map(fortnight)\
         .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))\
         .mapValues(lambda x: (x[0]/x[2], x[1]/x[2]))\
         .sortByKey()

#for period, val in rdd.collect():
#    print("15 Day Period Number: {}, Average Trip Cost: {:.2f}, Average Trip Distance: {:.2f}".format(period, val[0], val[1]))

rdd.saveAsTextFile("hdfs://master:9000/outputs/Q3_RDD.txt")
end = time.time()
print(end-start)

#save output time as .txt file
with open("Queries_exec_times.txt", "a") as f:
        f.write("Q3_RDD: " + str(end-start) + " sec\n")