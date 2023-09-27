from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pa

spark = SparkSession.builder.appName("Read CSV to HDFS").getOrCreate()

schema = StructType([ \
    StructField("phone_number",LongType(),False), \
    StructField("client_name",StringType(),False), \
    StructField("age",IntegerType(),False), \
    StructField('gender',IntegerType(),False) \
])

df = spark.read.csv("kurs/rtk_users.csv", header=False, schema = schema)
df = df.select("*")
df.write.mode('overwrite').parquet("hdfs://vm-dlake2-m-1.test.local/user/krasnickiy/wifi_data/rtk_users.parquet")


spark.stop()

