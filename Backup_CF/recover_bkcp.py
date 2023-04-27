import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader
import psycopg2
from psycopg2 import DatabaseError, pool, extras
import os



tables = {
    'jobs' : {
        'id': {
            'type': int,
            'required': True,
            'pk': True
        },
        'job': {
            'type': str,
            'required': True,
            'maxLength': 100,
            'default': None
        }
    },

    'departments' : {
        'id': {
            'type': int,
            'required': True,
            'pk': True
        },
        'department': {
            'type': str,
            'required': True,
            'maxLength': 100,
            'default': None
        }
    },

    'hiredemployees' : {
        'id': {
            'type': int,
            'required': True,
            'pk': True
        },
        'name': {
            'type': str,
            'required': True,
            'maxLength': 100,
            'default': None
        },
        'datetime': {
            'type': str,
            'required': True,
            'date': True,
        },
        'department_id': {
            'type': int,
            'required': True,
            'fk': [True, 'departments']
        },
        'job_id': {
            'type': int,
            'required': True,
            'fk': [True, 'jobs']
        }
    }
}

create_sql = {
    "departments" : """ CREATE TABLE employee.departments(
            id integer NOT NULL,
            department character varying(150),
            PRIMARY KEY(id)
            ); """,
    "jobs" : """ CREATE TABLE employee.jobs(
            id integer NOT NULL,
            job character varying(100),
            PRIMARY KEY(id)
            ); """,
    "hiredemployees": """ CREATE TABLE employee.hiredemployees(
            id integer NOT NULL,
            name character varying(100),
            datetime timestamp with time zone,
            department_id integer,
            job_id integer,
            PRIMARY KEY(id)
            ); """
}

def get_columns(table_name):
    if table_name in tables:
        return [col for col in list(tables[table_name].keys())]
    else:
        return 'Table does not exists'

def insert_sql( table_name):
    columns = get_columns(table_name)
    cols_str = ', '.join(col for col in columns)
    insert_into = "INSERT INTO employee.%s (%s) VALUES %%s;" % (table_name, cols_str)

    return insert_into

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
    
def recover_bkcp(db_schema, table, bckp_date):

    try:

        pool = tcp_pool_connection()
        conn = pool.getconn()
        with conn.cursor() as cursor:

            cursor = conn.cursor()
            file_name = f"{table}-{bckp_date}.avro"
            table_esquema = db_schema + "." + table
            #Read avro file
            reader = DataFileReader(open(f"./Backup_CF/avro_files/{file_name}", "rb"), DatumReader())
            #Execute
            table_values = [tuple(row.values()) for row in reader]
            cursor.execute(f'DROP TABLE IF EXISTS {table_esquema} CASCADE;')
            cursor.execute(create_sql.get(table))
            insert_query = insert_sql(table)
            try:
                extras.execute_values(cursor, insert_query, table_values, page_size=500)
                conn.commit()
            except Exception as ex:
                raise ex

    except Exception as ex:
        raise ex

recover_bkcp('employee','jobs', 20230426)