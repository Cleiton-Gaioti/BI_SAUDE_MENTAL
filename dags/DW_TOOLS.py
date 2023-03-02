from time import time
import sqlalchemy as sa


def create_connection(server, database, username, password, port):
    conn = f'postgresql+psycopg2://{username}:{password}@{server}:{port}/{database}'
    return sa.create_engine(conn)


def cronometrar(fun):
    def wrapper(*args, **kwargs):
        init = time()
        df = fun(*args, **kwargs)
        print(f'{fun.__name__}: {time() - init}')
        print(df.shape)

        return df
    return wrapper


def load_executemany(list_values, schema, table_name, ref_table, conn):
    query_insert = f"""
        INSERT INTO "{schema}"."{table_name}" ({", ".join(ref_table.keys())})
        VALUES ({", ".join(["%s" for v in range(1, len(ref_table.keys()) + 1)])})
    """

    connection = conn.raw_connection()

    conn_cursor = connection.cursor()

    conn_cursor.executemany(query_insert, list_values)

    conn_cursor.close()

    connection.commit()

    connection.close()


def create_struct_db(con, file):
    with open(file, 'r') as file:
        con.execute(file.read())


def truncate_table(con, schema, table_name):
    con.execute(f'TRUNCATE TABLE "{schema}"."{table_name}"')
