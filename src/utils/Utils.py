import config

tables = config.tables

class Utils():

    @classmethod
    def list_tuples(self, data):
        try:
            list_tuples = [tuple(row.values()) for row in data]
            return list_tuples
        except Exception as ex:
            raise (ex)

    @classmethod
    def validate_data(self, value, table_name, rules=tables):
        try:
            for key, rule in rules[table_name].items():
                val = value.get(key)
                # Check nullity
                if rule['required'] and val is None:
                    print(f'Field {key} is required')
                    return False

                # Check datatype
                if val is not None and not isinstance(val, rule['type']):
                    print(f"Field {key} must be of type {rule['type']}")
                    return False

                # Check str length
                if rule.get('maxLength') and len(val) > rule['maxLength']:
                    print(f"Failed {key} must be less than {rule['maxLength']}")
                    return False
                
            return True
        
        except Exception as ex:
            raise (ex)
