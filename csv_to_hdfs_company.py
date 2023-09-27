from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pa

spark = SparkSession.builder.appName("Read CSV to HDFS").getOrCreate()

schema = StructType([ \
    StructField("id",IntegerType(),False), \
    StructField("company_name",StringType(),False), \
    StructField("company_type",StringType(),False) \
])

df = spark.read.csv("kurs/company.csv", header=False, schema = schema)
df = df.select("*")
df.write.mode('overwrite').parquet("hdfs://vm-dlake2-m-1.test.local/user/krasnickiy/wifi_data/company.parquet")


spark.stop()

