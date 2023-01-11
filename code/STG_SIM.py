import os
import pandas as pd
from CONNECTION import create_connection


def extract_sim(sigla_estado=''):
    folder = '../datasources/sim_files/'
    init = f'ETLSIM.DORES_{sigla_estado}'

    df = pd.concat([pd.read_csv(folder+f, sep=',', low_memory=False) for f in os.listdir(folder) if f.startswith(init)])

    return df


def load_sim(df, conn):
    df.to_sql(name='SIM_ESPIRITO_SANTO', con=conn, schema='stg', if_exists='replace', chunksize=40000, method='multi')


if __name__ == '__main__':
    con_out = create_connection(server='localhost', database='tcc', username='cleiton', password='meutcc', port='5432')

    extract_sim('ES').pipe(load_sim, conn=con_out)
