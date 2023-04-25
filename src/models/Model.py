from multipledispatch import dispatch

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
                
            conn.close()
            pool.putconn(conn)
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
            #Data Quality
            verified_data = []
            print(data)
            for i, row in enumerate(data):
                if Utils.validate_data(row, table_name):
                    verified_data.append(row)
                else:
                    #Add logging
                    print(f'Row {i} does not pass the minimium quality requirements')
            with conn.cursor() as cursor:
                verified_data_tuple = Utils.list_tuples(verified_data)
                execute_values(cursor,insert_query,verified_data_tuple)
                affected_rows = cursor.rowcount
                conn.commit()
                conn.close()
                pool.putconn(conn)
            return affected_rows
        except Exception as ex:
            conn.close()
            pool.putconn(conn)
            raise ex