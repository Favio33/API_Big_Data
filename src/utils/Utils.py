import config

tables = config.tables


class Utils():

    # Verify if the key columns is unique in the table
    def __check_constraint(self, pool, value, column, table_name, _is_pk=True):
        conn = pool.getconn()
        with conn.cursor() as cursor:
            if _is_pk:
                cursor.execute(f'SELECT {column} FROM employee.{table_name}')
            else:
                fk = [id for id in tables[table_name]
                      if tables[table_name][id].get('pk')][0]
                cursor.execute(f'SELECT {fk} FROM employee.{table_name}')
            rows = cursor.fetchall()
            rows_list = [row[0] for row in rows]
            conn.close()
            pool.putconn(conn)
            if value in rows_list:
                return True
        return False

    @classmethod
    def list_tuples(self, data):
        try:
            list_tuples = [tuple(row.values()) for row in data]
            return list_tuples
        except Exception as ex:
            raise (ex)

    @classmethod
    def validate_data(self, row, table_name, pool, rules=tables):
        try:
            for column, rule in rules[table_name].items():
                val = row.get(column)
                # Check nullity
                if rule['required'] and val is None:
                    print(f'Field {column} is required')
                    return False

                # Check datatype
                if val is not None and not isinstance(val, rule['type']):
                    print(f"Field {column} must be of type {rule['type']}")
                    return False

                # Check str length
                if rule.get('maxLength') and len(val) > rule['maxLength']:
                    print(
                        f"Failed {column} must be less than {rule['maxLength']}")
                    return False

                # Check unique pk
                if rule.get('pk') and self.__check_constraint(self, pool, val, column, table_name):
                    print(f"Failed {column} must be unique in {table_name}")
                    return False
                

                if rule.get('fk') and self.__check_constraint(self, pool, val, column, rule.get('fk')[1], False):
                    print(f"Failed {column} constraint FK. Value must be register first on {table_name}")
                    return False
                
            return True

        except Exception as ex:
            raise (ex)
