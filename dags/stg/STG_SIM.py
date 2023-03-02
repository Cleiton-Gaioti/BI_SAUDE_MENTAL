import numpy as np
import pandas as pd
import DW_TOOLS as dwt
from pysus.online_data.SIM import download


@dwt.cronometrar
def extract_sim(uf, start_year, end_year):
    df = pd.concat([download(uf, year) for year in range(start_year, end_year + 1)])

    return df


@dwt.cronometrar
def treat_sim(df):
    df.columns = [col_name.lower() for col_name in df.columns]
    df = df.astype(str).replace({'': None, 'nan': None})

    return df


def load_sim(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='replace', index=False, chunksize=chunck, method='multi')


def run_sim(uf, start_year, end_year, con, schema, tb_name, chunck=10000):
    extract_sim(uf, start_year, end_year).pipe(treat_sim).pipe(load_sim, con, schema, tb_name, chunck)
