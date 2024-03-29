import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


# Drop table at first in case the tables already exist.
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

        
# Create all tables including both staging and final fact & dim tables.
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

        
# Connect to the database on the S3 and implement drop and create table queries.
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()