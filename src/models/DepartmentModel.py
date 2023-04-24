from database.conn import tcp_pool_connection

#Entitities
from .entitities.Departments import Departments


class DepartmentModel():

    @classmethod
    def get_depts(self):
        try:
            pool = tcp_pool_connection()
            conn = pool.getconn()
            depts = []

            with conn.cursor() as cursor:

                cursor.execute('SELECT * FROM employee.departments;')
                resultset = cursor.fetchall()
                
                for row in resultset:
                    dept = Departments(row[0], row[1])
                    depts.append(dept.to_JSON())
            conn.close()
            pool.putconn(conn)
            return depts
        except Exception as ex:
            raise Exception(ex)