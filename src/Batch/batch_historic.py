#!/usr/bin/env python
# coding: utf-8

## Imports


from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


import psycopg2
from psycopg2 import DatabaseError, pool
from psycopg2.extras import execute_values




## Utilities

def trim_string_fields(df):
    string_columns = [field[0] for field in df.dtypes if field[1] == 'string']
    for col in string_columns:
        df = df.withColumn(col, f.trim(f.col(col)))
    return df


def tcp_pool_connection():
    """
    Initializes a proxy connection for a Cloud SQL instance of Postgres.
    """
    INSTANCE_NAME='code-challenge-384515:us-central1:hr-db'
    PGSQL_USER='admin_prod'
    PGSQL_PASSWORD='pepo1612'
    PGSQL_DATABASE='employees'

    try:
        #Internal Variables to conenct through VPC
        host = '127.0.0.1'
        port = '5001'
        #Pool Connection
        pool = psycopg2.pool.SimpleConnectionPool(
        minconn=1,
        maxconn=5,
        host=host,
        port=port,
        user=PGSQL_USER,
        password=PGSQL_PASSWORD,
        database=PGSQL_DATABASE
        )

        return pool
    except DatabaseError as ex:
        print('Database Connection Failed!')
        raise ex


def sql_generator(table_name, key, df, cursor) -> str:
    
    all_cols = ', '.join(col for col in df.columns)
    update_cols = ', '.join(col for col in df.columns if col not in key)
    key_columns = ', '.join(col for col in key)
    place_holders = ', '.join('%s' for column in df.columns)
    insert_into = """ INSERT INTO %s (%s) VALUES %%s""" % (table_name, all_cols)
    update_set_query = ', '.join(f'{col}=excluded.{col}' for col in df.columns if col not in key)
    on_conflict_query = """ ON CONFLICT (%s) DO UPDATE SET %s;""" % (key_columns, update_set_query)
    
    return insert_into + on_conflict_query


def execute_upsert(pool, table_name, cols_excluded, df):
    conn = pool.getconn()
    with conn.cursor() as cursor:
        try:
            sql_stmt = sql_generator(table_name, cols_excluded, df, cursor)
            dfTuple = df.rdd.map(tuple).collect()
            execute_values(cursor, sql_stmt, dfTuple)
            conn.commit()
            pool.putconn(conn)
        except DatabaseError as ex:
            pool.putconn(conn)
            raise ex


# ## Schemas

#Define department schema
deptSchema = StructType([
    StructField('id', IntegerType(), True),
    StructField('department',StringType(), True)
])
#Define jobs schema
jobsSchema = StructType([
    StructField('id', IntegerType(), True),
    StructField('job',StringType(), True)
])
#Define hired employees schema
employeesSchema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name',StringType(), True),
    StructField('datetime',StringType(), True),
    StructField('department_id', IntegerType(), True),
    StructField('job_id',IntegerType(), False)
])


#SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

## Paths

#Define file's path on GCS
deptPath = 'gs://historic_files/departments.csv'
jobsPath = 'gs://historic_files/jobs.csv'
employeesPath = 'gs://historic_files/hired_employees.csv'


# ## DataPrep

dfDept = spark.read.format('csv').option('header','true').schema(deptSchema).load(deptPath)
dfJobs = spark.read.format('csv').option('header','true').schema(jobsSchema).load(jobsPath)
dfEmployees = spark.read.format('csv').option('header','true').schema(employeesSchema).load(employeesPath)


#Trim string columns
dfDept = trim_string_fields(dfDept)
dfJobs = trim_string_fields(dfJobs)
dfEmployees = trim_string_fields(dfEmployees)
#Fix Data Model
jobRow = spark.createDataFrame([(1,'Job not defined')],schema = jobsSchema)
dfJobs = dfJobs.union(jobRow).orderBy(f.col('id').asc())
deptRow = spark.createDataFrame([(1,'Department not defined')],schema = jobsSchema)
dfDept = dfDept.union(deptRow).orderBy(f.col('id').asc())
#Fix Null Values in Employees DF
dfEmployees = dfEmployees.fillna(1,subset=['department_id','job_id'])


# ## Load in Cloud SQL

pool = tcp_pool_connection()
#Bulk procces
execute_upsert(pool, 'employee.departments', ['id'], dfDept)
execute_upsert(pool, 'employee.jobs', ['id'], dfJobs)
execute_upsert(pool, 'employee.hiredEmployees', ['id'], dfEmployees)

