import configparser
from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, LongType, DecimalType

os.environ['PYSPARK_SUBMIT_ARGS'] = 'pyspark-shell'

config = configparser.ConfigParser()
config.read_file(open('/home/hadoop/project/dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

log = logging.getLogger("data_analysis.log")
log.info("Hello, world")

spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
    .enableHiveSupport() \
    .getOrCreate()

input_data = "s3a://ma2516-immigration-table/data"

# read in song data to use for songplays table
df_spark = spark.read.parquet(input_data)
df_spark.printSchema()
df_spark.show(5)

# create a view with the raw song data to use SQL to select columns
df_spark.createOrReplaceTempView("immigration_data_table")

columns = df_spark.columns

ignore_columns = ['cicid', 'i94yr', 'i94mon']
for ignore_column in ignore_columns:
    columns.remove(ignore_column)

for column in columns:
    select_sql = """
        select distinct({}), count(*)
        from immigration_data_table
        group by 1
        order by 2 desc
    """
    immigration_table = spark.sql(select_sql.format(column))
    immigration_table.show(100)



"""
column analysis

 |-- cicid: double (nullable = true)    drop column
 |-- i94yr: double (nullable = true)    int - all are 2016, remove nulls
 |-- i94mon: double (nullable = true)   varchar - all are from the month imported, remove nulls
 |-- i94cit: double (nullable = true)   int - country code, import description list from label desc file
 |-- i94res: double (nullable = true)   int - country code, import description list from label desc file
 |-- arrdate: double (nullable = true)  numeric - sas date field, convert to datetime
 |-- i94mode: double (nullable = true)  int - mode of arrival, build dim tbale from label desc file, very few nulls, remove nulls
 |-- i94addr: string (nullable = true)  varchar - state abbrev, import description list from label desc file, about 150K nulls, remove nulls and non matching values
 |-- depdate: double (nullable = true)  numeric - sas date field, convert to datetime, nearly 300k nulls
 |-- i94bir: double (nullable = true)   int - age field in years
 |-- i94visa: double (nullable = true)  int - visa code, build dim table from label desc file, no nulls
 |-- count: double (nullable = true)    drop column
 |-- dtadfile: string (nullable = true) drop column
 |-- visapost: string (nullable = true) varchar - dept of state where vis issued, 1.5M nulls, leave nulls
 |-- occup: string (nullable = true)    drop column, occupation, 2.5M nulls
 |-- entdepa: string (nullable = true)  varchar - can include but need info for dim table
 |-- entdepd: string (nullable = true)  varchar - can include but need info for dim table
 |-- entdepu: string (nullable = true)  drop column
 |-- matflag: string (nullable = true)  drop column
 |-- biryear: double (nullable = true)  int - birth year
 |-- dtaddto: string (nullable = true)  int - convert to date time, date admitted to us (allowed to stay till)
 |-- gender: string (nullable = true)   varchar - can include but need info for dim table
 |-- insnum: string (nullable = true)   drop column
 |-- airline: string (nullable = true)  varrchar - no way to translate, unless outside source data
 |-- admnum: double (nullable = true)   drop column
 |-- fltno: string (nullable = true)    int - flight number
 |-- visatype: string (nullable = true) varchar - no way to translate, unless outside source data
 |-- i94port: string (nullable = true)  varchar - airport code, import description list from label desc file



"""
