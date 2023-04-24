# Database
from database.conn import tcp_pool_connection

#Entities
from .entitities.Jobs import Jobs


class JobsModel():

    @classmethod
    def get_jobs(self):

        try:
            pool = tcp_pool_connection()
            conn = pool.getconn()
            jobs = []

            with conn.cursor() as cursor:
                cursor.execute('SELECT * FROM employee.jobs;')
                resultset = cursor.fetchall()
                
                for row in resultset:
                    job = Jobs(row[0], row[1])
                    jobs.append(job.to_JSON())
            conn.close()
            pool.putconn(conn)
            return jobs
        except Exception as ex:
            raise (ex)
