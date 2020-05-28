import configparser
import psycopg2
from sql_queries import staging_copy_queries, staging_table_list, insert_table_queries, insert_table_list
import time
import datetime
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


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.1.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def copy_sas(spark):
    """
    Description:
      Copies SAS data from S3 bucket to S3 bucket in Parquet form

    Parameters:
      spark - the spark instance cursor

    Returns:
      none
    """

    sas_data_in = config.get('SAS','INPUT_SAS_DATA')
    sas_data_out = config.get('SAS','OUTPUT_SAS_DATA')
    df_spark = spark.read.format('com.github.saurfang.sas.spark').option("inferSchema", "true").load(sas_data_in)

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
        select
            cicid, i94yr, i94mon, i94cit, i94res, i94port,
            cast(arrdate as int) arrdate, i94mode, i94addr,
            cast(depdate as int) depdate, i94bir, i94visa, count, dtadfile,
            visapost, occup, entdepa, entdepd, entdepu, matflag, biryear,
            cast(dtaddto as int) dtaddto, gender, insnum, airline, admnum,
            fltno, visatype
        from immigration_data_table
    ''')
    immigration_table.show()
    immigration_table.write.mode("overwrite").parquet(sas_data_out)


def load_staging_tables(cur, conn):
    """
    Description:
      Loads data from defined S3 bucket into staging tables. Use the
      sql_queries script to loop through each query defined in the
      copy_table_queries list.

    Parameters:
      cur - the db connection cursor
      conn - the db connection object

    Returns:
      none
    """
    for query in staging_copy_queries:
        print("Copying data to staging tables...")
        print()
        cur.execute(query)
        conn.commit()

    for table in staging_table_list:
        data_quality(table, cur, conn)


def load_csv_tables(cur, conn):
    """
    Description:
      Loads CSV data files from defined S3 bucket into staging tables.

    Parameters:
      cur - the db connection cursor
      conn - the db connection object

    Returns:
      none
    """

    csv_data = config.get('S3','CSV_DATA')
    iam_role = config.get('IAM_ROLE', 'ARN')

    csv_dict = {'ports': 'i94port',
                'countries': 'i94cnty',
                'modes': 'i94mode',
                'states': 'i94state',
                'visa_categories': 'i94visacat',
                'visa_types': 'i94visatype',
                'airlines': 'airlines_wiki',
                'demographics': 'us-cities-demographics'
               }

    for key, value in csv_dict.items():
        table = key
        file = value
        csv_source = csv_data + file + '.csv'

        query = ("""
            copy {}
            from '{}'
            credentials 'aws_iam_role={}'
            csv
            ignoreheader 1
        """).format(table, csv_source, iam_role)

        print(f"Copying CSV data from {file} into {table} table...")
        print()

        cur.execute(query)
        conn.commit()

        data_quality(table, cur, conn)


def insert_tables(cur, conn):
    """
    Description:
      Inserts transformed data from staging tables into the fact and dimension
      tables. Use the sql_queries script to loop through each query defined
      in the insert_table_queries list.

    Parameters:
      cur - the db connection cursor
      conn - the db connection object

    Returns:
      none
    """
    for query in insert_table_queries:
        print("Inserting data into fact/dim tables...")
        print()
        cur.execute(query)
        conn.commit()

    for table in insert_table_list:
        data_quality(table, cur, conn)


def data_quality(table, cur, conn):
    """
    Description:
      Checks data quality by verifying there is data present in the table
      after insert.

    Parameters:
      table - the table to check
      cur - the db connection cursor
      conn - the db connection object

    Returns:
      none
    """

    iam_role = config.get('IAM_ROLE', 'ARN')

    print(f"Performing data quality check on {table} table...")

    query = (f"select count(*) from {table}")
    cur.execute(query)
    conn.commit()
    records = cur.fetchone()

    if records[0] < 1:
        print(f"Data quality check failed! {table} table returned no results!")
    else:
        print(f"Data quality check on {table} table passed with {records[0]} records!")


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    spark = create_spark_session()
    copy_sas(spark)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    load_csv_tables(cur, conn)
    insert_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    start = datetime.datetime.now()
    start_dt = start.strftime("%Y-%m-%d %H:%M:%S")

    print()
    print('Starting the ETL script at {}'.format(start_dt))
    print()

    main()

    end = datetime.datetime.now()
    end_dt = end.strftime("%Y-%m-%d %H:%M:%S")

    print()
    print('ETL script complete at {}'.format(end_dt))
    print()
    print("Total execution time: {}".format(end - start))
    print()
