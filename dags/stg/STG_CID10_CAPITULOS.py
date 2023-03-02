import pandas as pd
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_cid10_capitulos(file, sep):
    return pd.read_csv(file, sep=sep)


@dwt.cronometrar
def treat_cid10_capitulos(df):
    df.columns = [col.lower() for col in df.columns]

    return df


def load_cid10_capitulos(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_cid10_capitulos(con, schema, tb_name, file=None, sep=";", chunck=10000):
    dwt.truncate_table(con, schema, tb_name)

    if not file:
        file = "https://github.com/bigdata-icict/ETL-Dataiku-DSS/blob/master/SIM/cid10_tabela_capitulos.csv?raw=true"

    extract_cid10_capitulos(file, sep).pipe(treat_cid10_capitulos).pipe(load_cid10_capitulos, con, schema, tb_name, chunck)
