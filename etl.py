"""Loads JSON data from S3 into Redshift database.

1) Loads JSON data into staging tables (~10 mins)
2) Transforms the data from staging into fact and dimension tables (~1 min)

Note: Run create_tables.py to create clean database before running this file.
"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# psycopg2 types
PGCursor: psycopg2.extensions.cursor = psycopg2.extensions.cursor
PGConnection: psycopg2.extensions.cursor = psycopg2.extensions.cursor


def load_staging_tables(cur: PGCursor, conn: PGConnection) -> None:
    """Loads JSON data from S3 into Redshift.

    Args:
        cur: database cursor
        conn: database connection
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur: PGCursor, conn: PGConnection) -> None:
    """Inserts data from staging tables into fact and dimension tables.

    Args:
        cur: database cursor
        conn: database connection
    """

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main() -> None:
    """Connects to Redshift database and populates the tables"""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
