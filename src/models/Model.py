#Logging
import logging

logging.basicConfig(filename='insertRows.log', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

#Database
from database.conn import tcp_pool_connection
from psycopg2.extras import execute_values

#Entititie
from models.entitities.Employees import Employees
from models.entitities.Departments import Departments
from models.entitities.Jobs import Jobs

#Utils
from utils.InsertQuery import InsertQuery
from utils.Utils import Utils
from utils.DataQuality import DataQuality
import config

tables = config.tables

class Model():

    def __set_employee(resultset:list):
        employees = []
        for row in resultset:
            employee = Employees(row[0], row[1], row[2], row[3], row[4])
            employees.append(employee.to_JSON())
        return employees
    
    def __set_jobs(resultset:list):
        jobs = []
        for row in resultset:
            job = Jobs(row[0], row[1])
            jobs.append(job.to_JSON())
        return jobs
    
    def __set_depts(resultset:list):
        departments = []
        for row in resultset:
            department = Departments(row[0], row[1])
            departments.append(department.to_JSON())
        return departments

    

    @classmethod
    def get_rows(self, table: str, numRows: int):

        try:
            
            pool = tcp_pool_connection()
            conn = pool.getconn()
            actions = {
                    'hiredemployees': self.__set_employee,
                    'jobs': self.__set_jobs,
                    'departments': self.__set_depts
                }   

            with conn.cursor() as cursor:
                sql_stmt = f"SELECT * FROM employee.{table} LIMIT %s"
                cursor.execute(sql_stmt, (numRows,))
                resultset = cursor.fetchall()

                if table in actions:
                    rows = actions[table](resultset)
                
            Utils.close_conn(pool,conn)
            return rows
            
        except Exception as ex:
            return Exception(ex)
    
    @classmethod
    def insert_rows(self, table_name, data):
        try:
            #Pool connection to postgre
            pool = tcp_pool_connection()
            conn = pool.getconn()
            
            #Build sql query
            insert_query = InsertQuery.insert_sql(table_name)

            #Constants
            verified_data = []
            entities_list = {
                'jobs': self.__set_jobs,
                'departments': self.__set_depts,
                'hiredemployees': self.__set_employee
            }

            if table_name not in tables:
                return "Not table in database"

            print(data)
            for i, row in enumerate(data):
                
                tuple_values = (tuple(row.values()),)

                try:
                    row_entitie = entities_list[table_name](tuple_values)[0]

                
                except Exception as ex:
                    return "Body request is not ok"

                logging.info(f"Verifying data quality row number {i}")
                if DataQuality.validate_data(row_entitie, table_name, pool):
                    #Data Quality
                    verified_data.append(row_entitie)
                    print(verified_data)
                else:
                    logging.warn(f"""Row number {i} not inserted due to data quality
                    Check up this dictionary: {row_entitie}""")
                    print(f'Row {i} does not pass the minimium quality requirements')


            if len(verified_data) >= 1:
                with conn.cursor() as cursor:
                    verified_data_tuple = Utils.list_tuples(verified_data)
                    execute_values(cursor,insert_query,verified_data_tuple)
                    affected_rows = cursor.rowcount
                    conn.commit()
                    Utils.close_conn(pool,conn)
                return affected_rows
            
            return 'Any row pass data quality requirements'
        except Exception as ex:
            Utils.close_conn(pool,conn)
            raise ex