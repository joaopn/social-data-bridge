"""Database connection and assertion helpers for E2E tests.

Provides simple wrappers around psycopg and pymongo for verifying
pipeline results in PostgreSQL and MongoDB.
"""


def pg_connect(port=5432, dbname="datasets", user="postgres", password=None):
    """Create a psycopg connection to PostgreSQL.

    Connects via localhost (the inner docker network publishes ports
    to the sysbox container's loopback).

    Args:
        port: PostgreSQL port.
        dbname: Database name.
        user: Database user.
        password: Optional password.

    Returns:
        psycopg connection object.
    """
    import psycopg

    conninfo = f"host=localhost port={port} dbname={dbname} user={user}"
    if password:
        conninfo += f" password={password}"
    return psycopg.connect(conninfo)


def pg_query_scalar(conn, query, params=None):
    """Execute a query and return the first column of the first row."""
    result = conn.execute(query, params)
    row = result.fetchone()
    return row[0] if row else None


def pg_table_exists(conn, schema, table):
    """Check if a table exists in a given schema."""
    return pg_query_scalar(
        conn,
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s)",
        (schema, table),
    )


def pg_row_count(conn, schema, table):
    """Return row count for a table."""
    return pg_query_scalar(conn, f'SELECT count(*) FROM "{schema}"."{table}"')


def pg_index_count(conn, schema):
    """Return number of indexes in a schema (excluding PK)."""
    return pg_query_scalar(
        conn,
        "SELECT count(*) FROM pg_indexes WHERE schemaname = %s",
        (schema,),
    )


def mongo_connect(port=27017, username=None, password=None):
    """Create a pymongo MongoClient.

    Args:
        port: MongoDB port.
        username: Optional username.
        password: Optional password.

    Returns:
        pymongo.MongoClient instance.
    """
    from pymongo import MongoClient

    if username and password:
        uri = f"mongodb://{username}:{password}@localhost:{port}/?authSource=admin"
    else:
        uri = f"mongodb://localhost:{port}/"
    return MongoClient(uri)


def mongo_doc_count(client, db_name, collection_name):
    """Return document count for a collection."""
    return client[db_name][collection_name].count_documents({})


def mongo_collection_exists(client, db_name, collection_name):
    """Check if a collection exists in a database."""
    return collection_name in client[db_name].list_collection_names()
