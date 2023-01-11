import sqlalchemy as sa


def create_connection(server, database, username, password, port):
    conn = f'postgresql+psycopg2://{username}:{password}@{server}:{port}/{database}'
    return sa.create_engine(conn)
