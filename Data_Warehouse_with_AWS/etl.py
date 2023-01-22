import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


# Run staging queries to load json data from S3 into staging tables in redshift
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


# Run inserting queries to load staged data into fact and dimension tables
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # Connect to the database.
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # Load json data into staging tables and then insert staged data into fact and dimension tables.
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    # Close connection to the database
    conn.close()


if __name__ == "__main__":
    main()