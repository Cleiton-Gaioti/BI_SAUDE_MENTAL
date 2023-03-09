from time import time
import sqlalchemy as sa
from io import StringIO


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


def load_with_csv(df, con, schema, tb_name):
    cols = [f'"{col}"' for col in df.columns]
    cols.sort()

    connection = con.raw_connection()
    cur = connection.cursor()
    output = StringIO()

    df.to_csv(output, sep='|', header=False, index=False)

    output.seek(0)
    cur.copy_expert(f"COPY {schema}.{tb_name} ({', '.join(cols)}) FROM STDIN (DELIMITER '|', NULL '')", output)
    connection.commit()

    output.close()
    cur.close()
    connection.close()


def create_struct_db(con, file):
    with open(file, 'r') as file:
        con.execute(file.read())


def truncate_table(con, schema, table_name):
    con.execute(f'TRUNCATE TABLE "{schema}"."{table_name}"')


def get_table_columns(con, schema, tb_name):
    query = f"""
        SELECT DISTINCT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{tb_name}'
    """

    result = con.execute(query)
    cols = [r[0] for r in result.fetchall()]

    return cols
