import pandas as pd
import pandas.io.sql as psql

# Database
from database.conn import tcp_pool_connection


class Processing():


    def __generate_response(df):
        list_response = []
        for row in df.values:
            val = dict()
            columns = df.columns
            val[columns[0]] = row[0]
            val[columns[1]] = row[1]
            val[columns[2]] = row[2]
            val[columns[3]] = row[3]
            val[columns[4]] = row[4]
            val[columns[5]] = row[5]
            list_response.append(val)
        print("Done")
        return list_response

    @classmethod
    def resumen_quarter(self, year):
        try:
            pool = tcp_pool_connection()
            conn = pool.getconn()

            dfEmployee = psql.read_sql(
                'SELECT id, datetime, job_id, department_id FROM employee.hiredemployees;', conn)
            dfJobs = psql.read_sql('SELECT * FROM employee.jobs;', conn)
            dfDepts = psql.read_sql(
                'SELECT * FROM employee.departments;', conn)

            #Left Joins between dataframes
            dfLeftEmployeeJobs = dfEmployee.merge(
                dfJobs, how='left', left_on='job_id', right_on='id').drop(columns=['id_y']).copy()
            dfLeftEmployeeDepts = dfLeftEmployeeJobs.merge(
                dfDepts, how='left', left_on='department_id', right_on='id').drop(columns=['id']).copy()
            
            #Filtered df according a certain year
            dfFilteredLeftEmployeeDepts = dfLeftEmployeeDepts[dfLeftEmployeeDepts['datetime'].dt.strftime(
                "%Y") == str(year)].drop(columns=['department_id', 'job_id'])
            #Get quarter
            dfFilteredLeftEmployeeDepts['quarter'] = "Q" + \
                dfFilteredLeftEmployeeDepts['datetime'].dt.quarter.map(str)
            #Pivot Table
            pivotTable = pd.pivot_table(dfFilteredLeftEmployeeDepts, index=[
                                        'department', 'job'], columns='quarter', values='id_x', aggfunc='count').fillna(0).astype('int8').reset_index(drop=False)
            #Order table
            orderedPivotTable = pivotTable.sort_values(by=['department','job'])

            #Generate response
            dict_values = self.__generate_response(orderedPivotTable)

            return dict_values

        except Exception as ex:
            raise (ex)
