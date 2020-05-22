import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import time
import datetime


def drop_tables(cur, conn):
    """
    Description:
      Drops all existing tables from the database.  Use the sql_queries script
      to loop through each query defined in the drop_table_queries list.
    Parameters:
      cur - the db connection cursor
      conn - the db connection object
    Returns:
      none
    """
    for query in drop_table_queries:
        print("Dropping tables...")
        print()
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description:
      Creates required new tables.  Use the sql_queries script
      to loop through each query defined in the create_table_queries list.
    Parameters:
      cur - the db connection cursor
      conn - the db connection object
    Returns:
      none
    """
    for query in create_table_queries:
        print("Creating tables...")
        print()
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    start = datetime.datetime.now()
    start_dt = start.strftime("%Y-%m-%d %H:%M:%S")

    print()
    print('Starting the table creation script at {}'.format(start_dt))
    print()

    main()

    end = datetime.datetime.now()
    end_dt = end.strftime("%Y-%m-%d %H:%M:%S")

    print()
    print('Table creation script complete at {}'.format(end_dt))
    print()
    print("Total execution time: {}".format(end - start))
    print()
