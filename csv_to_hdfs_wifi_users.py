from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

spark = SparkSession.builder.appName("Read CSV to HDFS").getOrCreate()

try: 

    schema = StructType([ \
        StructField("phone_number",LongType(),False), \
        StructField("device",StringType(),False), \
        StructField("ident",StringType(),False), \
        StructField("session_start",TimestampType(),False), \
        StructField('session_duration',IntegerType(),False), \
        StructField('traffic',IntegerType(), False), \
        StructField('company_id',IntegerType(),False) \
    ])

    df = spark.read.csv("kurs/wifi_users.csv", header=False, schema = schema)
    #df = df.select("*").limit(10)
    df.write.mode('append').parquet("hdfs://vm-dlake2-m-1.test.local/user/krasnickiy/wifi_data/wifi_users.parquet")

except Exception as e:
    print(e)

finally:

    spark.stop()
