import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, alter_table_queries


def drop_tables(cur, conn):
    """
    Reads the list of SQL queries and iterate over them to drop tables, database cursor and 
    connection objects passed into this function.
    cur -- Database cursor object
    conn -- Database connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Reads the list of SQL queries and iterate over them to create tables, database cursor and 
    connection objects passed into this function.
    cur -- Database cursor object
    conn -- Database connection object
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def alter_tables(cur, conn):
    """
    Reads the list of SQL queries and iterate over them alter tables, database cursor and 
    connection objects passed into this function.
    cur -- Database cursor object
    conn -- Database connection object
    """

    for query in alter_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Function reads a dwh.cfg configuration file, and uses credentials provided in the config file to 
    create a connection to a specified database in Amazon Redshift.
    It calls two nested functions for executing SQL queries against the database connection, before
    closing the connection.
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