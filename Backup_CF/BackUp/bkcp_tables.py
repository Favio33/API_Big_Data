import avro.schema
import psycopg2
from psycopg2 import DatabaseError, pool
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import os
import datetime


# Connection
type_map = {
    "integer": "int",
    "character varying": 'string',
    'timestamp with time zone': 'string',
    'decimal': 'float',
    'smallint': 'int',
    'date': 'string'
}


def __get_tables(cursor, db_schema):
    cursor.execute(
        f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db_schema}'")
    return [row[0] for row in cursor.fetchall()]

def __gen_avro_schema(col_datatype_tuple, table):
    fields_schema = [
        '{"name": "' + col[0] + '", "type": "' + type_map[col[1]] + '"}' for col in col_datatype_tuple]
    
    schema_str = '{"type":"record", "name": "' + table + \
        '", "namespace": "employee", "fields": [' + \
        ', '.join(fields_schema) + ']}'

    return avro.schema.parse(schema_str)

def tcp_pool_connection():
    """
    Initializes a proxy connection for a Cloud SQL instance of Postgres.
    """

    try:
        if os.environ.get('GAE_ENV') == 'standard':
            host = '/cloudsql/{}'.format(os.environ.get('INSTANCE_NAME'))
            port = '5432'
        else:
            host = os.environ.get('PGSQL_LOCALHOST')
            port = '3306'

        pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=host,
            port=port,
            user=os.environ.get('PGSQL_USER'),
            password=os.environ.get('PGSQL_PASSWORD'),
            database=os.environ.get('PGSQL_DATABASE')
        )

        return pool
    except DatabaseError as ex:
        print('Database Connection Failed!')
        raise ex


def backup_tables(db_schema, backup_path):

    try:

        pool = tcp_pool_connection()
        conn = pool.getconn()

        with conn as conn:

            cursor = conn.cursor()
            # Get All Tables
            tables = __get_tables(cursor,db_schema)
            cursor.close()

            for table in tables:

                cursor = conn.cursor()
                table_esquema = db_schema + "." + table
                cursor.execute(
                    f"select column_name, data_type from information_schema.columns where table_name = %s and table_schema = %s;", (table, db_schema))

                columns = cursor.fetchall()

                # Generate avro schema
                schema = __gen_avro_schema(columns, table)

                with DataFileWriter(open(f"{backup_path}/{table}-{datetime.datetime.now().strftime('%Y-%m-%dT%H%M%SZ')}.avro", "wb"), DatumWriter(), schema) as writer:
                    cursor.execute(f"SELECT * FROM {table_esquema}")
                    rows = cursor.fetchall()
                    for row in rows:
                        formatted_row = [val.strftime('%Y-%m-%dT%H:%M:%SZ') if isinstance(val,datetime.datetime) else val for val in row]
                        writer.append(
                            dict(zip([col[0] for col in columns], formatted_row)))

                cursor.close()

    except Exception as ex:
        raise (ex)


backup_tables('employee', './Backup_CF/avro_files')
