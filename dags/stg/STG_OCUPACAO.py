import pandas as pd
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_stg_ocupacao(file_path, sep):
    return pd.read_csv(file_path, sep=sep)


@dwt.cronometrar
def treat_stg_ocupacao(df):
    df.columns = [col.lower() for col in df.columns]
    df = df.astype({'cod': 'Int64', 'nome': str})

    return df


def load_stg_ocupacao(df, con, schema, tb_name):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='replace', index=False, chunksize=10000, method='multi')


def run_stg_ocupacao(con, schema, tb_name, file_path=None, sep=";"):
    dwt.truncate_table(con, schema, tb_name)

    if not file_path:
        file_path = "https://github.com/rfsaldanha/microdatasus/blob/master/data-raw/tabOcupacao.csv?raw=true"

    extract_stg_ocupacao(file_path, sep).pipe(treat_stg_ocupacao).pipe(load_stg_ocupacao, con, schema, tb_name)
