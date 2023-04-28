import pandas as pd
import pandas.io.sql as psql

# Database
from database.conn import tcp_pool_connection


class Processing():

    def __joint_tables(self, dfJobs, dfEmployee, dfDepts):
        # Left Joins between dataframes
        dfLeftEmployeeJobs = dfEmployee.merge(
            dfJobs, how='left', left_on='job_id', right_on='id', suffixes=(None,'_x')).drop(columns=['id_x']).copy()
        dfLeftEmployeeDepts = dfLeftEmployeeJobs.merge(
            dfDepts, how='left', left_on='department_id', right_on='id', suffixes=(None,'_x')).drop(columns=['department_id', 'job_id', 'id_x']).copy()
        return dfLeftEmployeeDepts

    def __generate_response(self, df):
        list_response = []
        for row in df.values:
            val = dict()
            i=0
            for col in df.columns:
                val[col] = row[i]
                i=i+1
            list_response.append(val)
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

            # Join Tables
            dfLeftJoinTables = self.__joint_tables(
                self, dfJobs, dfEmployee, dfDepts)

            # Filtered df according a certain year
            dfFilteredLeftEmployeeDepts = dfLeftJoinTables[dfLeftJoinTables['datetime'].dt.strftime(
                "%Y") == str(year)]
            # Get quarter
            dfFilteredLeftEmployeeDepts['quarter'] = "Q" + \
                dfFilteredLeftEmployeeDepts['datetime'].dt.quarter.map(str)
            # Pivot Table
            pivotTable = pd.pivot_table(dfFilteredLeftEmployeeDepts, index=[
                                        'department', 'job'], columns='quarter', values='id', aggfunc='count').fillna(0).astype('int8').reset_index(drop=False)
            # Order table
            orderedPivotTable = pivotTable.sort_values(
                by=['department', 'job'])

            # Generate response
            dict_values = self.__generate_response(self, orderedPivotTable)

            return dict_values

        except Exception as ex:
            raise (ex)

    @classmethod
    def report_hired_employes_top(self):
        try:
            pool = tcp_pool_connection()
            conn = pool.getconn()

            dfEmployee = psql.read_sql(
                'SELECT id, datetime, job_id, department_id FROM employee.hiredemployees;', conn)
            dfJobs = psql.read_sql('SELECT * FROM employee.jobs;', conn)
            dfDepts = psql.read_sql(
                'SELECT * FROM employee.departments;', conn)

            # Join Tables
            dfLeftJoinTables = self.__joint_tables(
                self, dfJobs, dfEmployee, dfDepts)[['id','department','datetime']]
            # Filtered df according a certain year
            dfFilteredLeftEmployeeDepts = dfLeftJoinTables[dfLeftJoinTables['datetime'].dt.strftime(
                "%Y") == str(2021)].iloc[:,:2]
            #Mean hired employees for all departments in 2022
            mean_2021 = int(dfFilteredLeftEmployeeDepts.groupby('department').count().mean().round(0).iloc[0])
            #Report
            dfLeftJoinTablesGrouped =  dfLeftJoinTables.iloc[:,:2].groupby('department').count().astype(int).reset_index(drop=False)
            dfFilterGroupedTable = dfLeftJoinTablesGrouped[dfLeftJoinTablesGrouped['id'] > mean_2021].rename({'id':'hired'})
            #Response
            dict_values = self.__generate_response(self, dfFilterGroupedTable)

            return dict_values
        except Exception as ex:
            raise ex
