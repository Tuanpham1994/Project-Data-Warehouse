import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Create function to load staging table query"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    
#Create function to insert table query
def insert_tables(cur, conn):
    """function to insert table query"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """function to connect with cluster"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()