import pandas as pd
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_stg_uf():
    return pd.read_json('https://servicodados.ibge.gov.br/api/v1/localidades/estados?view=nivelado', orient='records')


@dwt.cronometrar
def treat_stg_uf(df):
    df.columns = [col.lower() for col in df.columns]

    return df.astype({"uf-id": "Int64", "regiao-id": "Int64"})


def load_stg_uf(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_stg_uf(con, schema, tb_name, chunck=10000):
    dwt.truncate_table(con, schema, tb_name)

    extract_stg_uf().pipe(treat_stg_uf).pipe(load_stg_uf, con, schema, tb_name, chunck)
