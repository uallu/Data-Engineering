import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    ''' This function is responsible for loading the files from S3 bucket to the stage tables. 
    > cur, cursor varilabe for the databse
    -> con, connection varibale for the database'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    ''' This function goes over the list of insert queries which inserts the data from stage table to destination table.
    > cur, cursor varilabe for the databse
    -> con, connection varibale for the database'''
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    ''' Entry point of the program. Reads the credentials from the config file, connects to database
    loads the files from S3 to stage and from stage to destination table'''
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()