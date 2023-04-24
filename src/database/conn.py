import os
from decouple import config
import psycopg2
import psycopg2.pool
from psycopg2 import DatabaseError


def tcp_pool_connection():
    """
    Initializes a proxy connection for a Cloud SQL instance of Postgres.
    """

    try:
        if os.environ.get('GAE_ENV') == 'standard':
            host = '/cloudsql/{}'.format(config('INSTANCE_NAME'))
            port = '5432'
        else:
            host = config('PGSQL_LOCALHOST')
            port = '3306'

        pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=host,
            port=port,
            user=config('PGSQL_USER'),
            password=config('PGSQL_PASSWORD'),
            database=config('PGSQL_DATABASE')
        )

        return pool
    except DatabaseError as ex:
        print('Database Connection Failed!')
        raise ex
