#Variables
import config
import sys

sys.path.append("..")

tables = config.tables

class InsertQuery():

    def __get_columns(self, table_name):
        if table_name in tables:
            return [col for col in list(tables[table_name].keys())]
        else:
            return 'Table does not exists'
    
    @classmethod
    def insert_sql(self, table_name):
        columns = self.__get_columns(self, table_name)
        cols_str = ', '.join(col for col in columns)
        insert_into = "INSERT INTO employee.%s (%s) VALUES %%s;" % (table_name, cols_str)

        return insert_into

