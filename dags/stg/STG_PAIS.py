import pandas as pd
import DW_TOOLS as dwt


def extract_stg_pais():
    return pd.read_json('https://servicodados.ibge.gov.br/api/v1/localidades/paises?view=nivelado', orient='records')


def treat_stg_pais(df):
    df.columns = [col.lower() for col in df.columns]

    df = df.astype({'pais-m49': 'Int64', 'regiao-intermediaria-m49': 'Int64', 'sub-regiao-m49': 'Int64', 'regiao-m49': 'Int64'})

    return df


def load_stg_pais(df, con, schema, tb_name):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='replace', index=False, chunksize=10000, method='multi')


def run_stg_pais(con, schema, tb_name):
    dwt.truncate_table(con, schema, tb_name)

    extract_stg_pais().pipe(treat_stg_pais).pipe(load_stg_pais, con, schema, tb_name)