import pandas as pd
import DW_TOOLS as dwt
from ibge.localidades import Estados
from pysus.online_data.SIM import download


@dwt.cronometrar
def extract_sim(uf, year):
    df = download(uf, year)

    return df


def treat_sim(df, columns):
    df.columns = [col_name.lower() for col_name in df.columns]

    for col in columns:
        if col not in df.columns:
            df.insert(0, col, None)

    df = df[columns]

    df = df.astype(str).replace({'': None, 'nan': None, 'null': None, 'NULL': None, 'None': None})

    return df


def load_sim(df, con, schema, tb_name):
    dwt.load_with_csv(df, con, schema, tb_name)


def run_sim(ufs, start_year, con, schema, tb_name, end_year=None, chunck=10000):
    dwt.truncate_table(con, schema, tb_name)
    columns = dwt.get_table_columns(con, schema, tb_name)

    if not end_year:
        end_year = start_year

    if isinstance(ufs, str):
        if ufs.lower() == 'all':
            ufs = Estados().getSigla()
        else:
            ufs = [ufs]

    [[extract_sim(uf, year).pipe(treat_sim, columns).pipe(load_sim, con, schema, tb_name) for year in range(start_year, end_year+1)] for uf in ufs]