import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, LongType, DecimalType

config = configparser.ConfigParser()
config.read_file(open('/home/hadoop/project/dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.1.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
    .enableHiveSupport() \
    .getOrCreate()

#df_spark = spark.read.csv('s3a://ma2516-immigration-data/immigration_data_sample.csv', header=True, inferSchema=True)                                   # verified this works
df_spark = spark.read.format('com.github.saurfang.sas.spark').option("inferSchema", "true").load('s3a://ma2516-immigration-data/i94_feb16_sub.sas7bdat') # verified this method works
#df_spark = spark.read.format('com.github.saurfang.sas.spark').option("inferSchema", "true").load('file:///home/hadoop/i94_apr16_sub.sas7bdat')          # verified this works!

df_spark.show(5)
df_spark.printSchema()

# create a view with the raw song data to use SQL to select columns
df_spark.createOrReplaceTempView("immigration_data_table")

immigration_count = spark.sql('''
    select count(*)
    from immigration_data_table
''')
immigration_count.show()

immigration_table = spark.sql('''
    select *
    from immigration_data_table
    limit 100
''')
immigration_table.show()

#immigration_table.write.mode("overwrite").partitionBy("i94port").parquet("s3a://ma2516-immigration-table/data")
immigration_table.write.mode("overwrite").parquet("s3a://ma2516-immigration-table/data")
