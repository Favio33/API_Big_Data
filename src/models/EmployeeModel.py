#Database
from database.conn import tcp_pool_connection

#Entititie
from models.entitities.Employees import Employees


class EmployeeModel():

    @classmethod
    def get_employees(self, numRows: int):

        try:
            
            pool = tcp_pool_connection()
            conn = pool.getconn()
            employees=[]

            with conn.cursor() as cursor:
                sql_stmt = "SELECT * FROM employee.hiredemployees LIMIT %s"
                cursor.execute(sql_stmt, (numRows,))
                resultset = cursor.fetchall()
                for row in resultset:
                    employee = Employees(row[0], row[1], row[2], row[3], row[4])
                    employees.append(employee.to_JSON())
            conn.close()
            pool.putconn(conn)
            return employees
        except Exception as ex:
            raise Exception(ex)