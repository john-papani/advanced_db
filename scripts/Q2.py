from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
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

start = time.time()

df2 = df.withColumn('month',month(df.tpep_pickup_datetime))\
        .filter((~(df['Tolls_amount']==0)))

w = Window.partitionBy('month')

dfQ2 = df2.withColumn('max_Tolls_amount', max('Tolls_amount').over(w))\
          .where(col('Tolls_amount') == col('max_Tolls_amount'))\
          .drop('max_Tolls_amount')

dfQ2.write.parquet("hdfs://master:9000/outputs/Q2.parquet")

end = time.time()

#save output time as .txt file
with open("Queries_exec_times.txt", "a") as f:
        f.write("Q2: {} sec\n".format(end-start))