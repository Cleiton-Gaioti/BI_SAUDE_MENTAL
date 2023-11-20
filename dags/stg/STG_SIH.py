import pandas as pd
import DW_TOOLS as dwt
from ibge.localidades import Estados
from pysus.online_data.SIH import download


def extract_sih(uf, year):
    months = [i for i in range(1,13)]
    path_list = download(states=uf, years=year, months=months)

    return pd.concat([pd.read_parquet(path) for path in path_list])


@dwt.cronometrar
def treat_sih(df, columns):
    df.columns = [col_name.lower() for col_name in df.columns]

    for col in columns:
        if col not in df.columns:
            df.insert(0, col, None)

    df = df.astype('object').apply(lambda x: x.str.strip()).replace('', None)

    return df[columns]


def load_sih(df, con, schema, tb_name, columns):
    dwt.load_with_csv(df, con, schema, tb_name, columns)


def run_sih(ufs, start_year, con, schema, tb_name, end_year=0):
    dwt.truncate_table(con, schema, tb_name)

    columns = dwt.get_table_columns(con, schema, tb_name)
    years = list(range(start_year, max(start_year, end_year) + 1))

    if isinstance(ufs, str):
        if ufs.lower() == 'br':
            ufs = Estados().getSigla()
        else:
            ufs = [ufs]

    [[extract_sih(uf, year).pipe(treat_sih, columns).pipe(load_sih, con, schema, tb_name, columns) for year in years] for uf in ufs]