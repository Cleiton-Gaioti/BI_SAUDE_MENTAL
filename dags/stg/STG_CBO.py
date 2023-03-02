import numpy as np
import pandas as pd
from DW_TOOLS import cronometrar


@cronometrar
def extract_stg_cbo(file_path, sep):
    return pd.read_csv(file_path, sep=sep)


@cronometrar
def treat_stg_cbo(df):
    df.columns = [col.lower() for col in df.columns]
    df = df.astype({'cod': 'Int64', 'nome': str})

    return df


def load_stg_cbo(df, con, schema, tb_name):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='replace', index=False, chunksize=10000, method='multi')


def run_stg_cbo(file_path, con, schema, tb_name, sep=";"):
    extract_stg_cbo(file_path, sep).pipe(treat_stg_cbo).pipe(load_stg_cbo, con, schema, tb_name)
