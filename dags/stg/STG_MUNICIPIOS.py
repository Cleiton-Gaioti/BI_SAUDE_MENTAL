import pandas as pd
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_stg_municipios():
    return pd.read_json('https://servicodados.ibge.gov.br/api/v1/localidades/municipios?view=nivelado', orient='records')


@dwt.cronometrar
def treat_stg_municipios(df):
    df.columns = [col.lower() for col in df.columns]

    dtypes = {
        "municipio-id": "Int64", 
        "microrregiao-id": "Int64", 
        "mesorregiao-id": "Int64", 
        "regiao-imediata-id": "Int64", 
        "regiao-intermediaria-id": "Int64", 
        "uf-id": "Int64", 
        "regiao-id": "Int64"}

    df = df.astype(dtypes)

    return df


def load_stg_municipios(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_stg_municipios(con, schema, tb_name, chunck=10000):
    dwt.truncate_table(con, schema, tb_name)

    extract_stg_municipios().pipe(treat_stg_municipios).pipe(load_stg_municipios, con, schema, tb_name, chunck)
