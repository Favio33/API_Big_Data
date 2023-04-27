import config

tables = config.tables


class DataQuality():

    # Verify if the key columns is unique in the table
    def __check_constraint(self, pool, value, column, table_name, _is_pk=True):
        with pool.getconn() as conn:
            cursor = conn.cursor()
            if _is_pk:
                cursor.execute(f'SELECT {column} FROM employee.{table_name}')
            else:
                fk = [id for id in tables[table_name]
                      if tables[table_name][id].get('pk')][0]
                print(f'SELECT {fk} FROM employee.{table_name}')
                cursor.execute(f'SELECT {fk} FROM employee.{table_name}')
            rows = cursor.fetchall()
            rows_list = [row[0] for row in rows]
            cursor.close()
            pool.putconn(conn)
            if value in rows_list:
                return False
        return False

    @classmethod
    def validate_data(self, row, table_name, pool, rules=tables):
        try:
            for column, rule in rules[table_name].items():
                val = row.get(column)
                print(val)
                # Check nullity
                if rule['required'] and val is None:
                    print(f'Field {column} is required')
                    return False
                print('1')
                # Check datatype
                if val is not None and not isinstance(val, rule['type']):
                    print(f"Field {column} must be of type {rule['type']}")
                    return False
                print('2')
                # Check str length
                if rule.get('maxLength') and len(val) > rule['maxLength']:
                    print(
                        f"Failed {column} must be less than {rule['maxLength']}")
                    return False
                print('3')
                # Check unique pk
                if rule.get('pk') and self.__check_constraint(self, pool, val, column, table_name):
                    print(f"Failed {column} must be unique in {table_name}")
                    return False
                print('4')
                if rule.get('fk') and self.__check_constraint(self, pool, val, column, rule.get('fk')[1], False):
                    print(rule.get('fk')[1])
                    print(
                        f"Failed {column} constraint FK. Value must be register first on {table_name}")
                    return False
                print('5')
            return True

        except Exception as ex:
            raise (ex)
