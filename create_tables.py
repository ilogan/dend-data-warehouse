"""Creates tables in Redshift database

Connects to Redshift using credentials in config file.
Deletes any existing tables, then adds empty tables
to Redshift.
"""
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# psycopg2 types
PGCursor: psycopg2.extensions.cursor = psycopg2.extensions.cursor
PGConnection: psycopg2.extensions.cursor = psycopg2.extensions.cursor


def drop_tables(cur: PGCursor, conn: PGConnection) -> None:
    """Drop the previously created tables if they exist

    Args:
        cur: database cursor
        conn: database connection
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur: PGCursor, conn: PGConnection) -> None:
    """Create empty staging, fact, and dimension tables

    Args:
        cur: database cursor
        conn: database connection
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main() -> None:
    """Connects to Redshift database, then drops any previously created
    tables before remaking them as empty tables.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
