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
