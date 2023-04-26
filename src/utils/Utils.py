
class Utils():

    @classmethod
    def list_tuples(self, data):
        try:
            list_tuples = [tuple(row.values()) for row in data]
            return list_tuples
        except Exception as ex:
            raise (ex)

    @classmethod
    def close_conn(self, pool, conn):
        conn.close()
        pool.putconn(conn)
