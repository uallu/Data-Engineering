import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    ''' This function goes over the drop table queries from the list and executes them. 
    Input: 
    -> cur, cursor varilabe for the databse
    -> con, connection varibale for the database'''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
     ''' This function goes over the create table queries from the list and executes them. 
    Input: 
    -> cur, cursor varilabe for the databse
    -> con, connection varibale for the database'''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Entry point of the program. Connects to database using credentials in the config file and then drops
        and creates/recreates the required tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()