import os
import pandas as pd
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


def read_table_from_sql(query, con, chunck=None):
    with con.connect() as conn:
        query = sa.text(query)

        if chunck:
            return [df for df in pd.read_sql_query(query, conn, chunksize=chunck)]
        else:
            return pd.read_sql_query(query, conn)
        

def read_sql_query(query, con):
    with con.connect() as conn:
        return conn.execute(sa.text(query))


def load_with_csv(df, con, schema, tb_name, columns, test=False):
    cols = [f'"{col}"' for col in columns]

    if test:
        df.to_csv(f'dags/{tb_name}.csv', sep='|', index=False)
    else:
        connection = con.raw_connection()
        
        with connection.cursor() as cur:
            with StringIO() as output:
                df.to_csv(output, sep='|', header=False, index=False)
                
                output.seek(0)
                print(df.columns)
                cur.copy_expert(f"COPY {schema}.{tb_name} ({', '.join(cols)}) FROM STDIN (DELIMITER '|', NULL '')", output)
        
        connection.commit()
        connection.close()


def get_max_sk(con, schema, tb_name, col_name):
    with con.connect() as conn:
        query = sa.text(f"SELECT MAX({col_name}) FROM {schema}.{tb_name}")
        result = conn.execute(query).fetchone()[0]

        return result if result else 0
    

def get_dimension_size(con, schema, tb_name):
    with con.connect() as conn:
        query = sa.text(f'SELECT COUNT(1) FROM {schema}.{tb_name}')

        return conn.execute(query).fetchone()[0]


def create_struct_db(con, file):
    with open(file, 'r') as file:
        con.execute(file.read())


def truncate_table(con, schema, table_name):
    with con.connect() as conn:
        query = sa.text(f'TRUNCATE TABLE "{schema}"."{table_name}"')
        conn.execute(query)


def get_table_columns(con, schema, tb_name):
    with con.connect() as conn:
        query = sa.text(f"""
            SELECT DISTINCT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{tb_name}'
        """)

        result = conn.execute(query)

        return [r[0] for r in result.fetchall()]
    

def convert_date(str_date):
    try:
        return pd.to_datetime(str_date)
    except:
        return None
