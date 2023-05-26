import pandas as pd
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_stg_leitos(file_path, sep):
    return pd.read_csv(file_path, sep=sep)


@dwt.cronometrar
def treat_stg_leitos(df):
    df.columns = [col.lower() for col in df.columns]
    df = df.astype({'cod': 'Int64', 'value': str})

    return df


def load_stg_leitos(df, con, schema, tb_name):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=10000, method='multi')


def run_stg_leitos(con, schema, tb_name, file_path=None, sep=";"):
    dwt.truncate_table(con, schema, tb_name)

    if not file_path:
        file_path = "dags/arquivos_auxiliares/LEITOS.csv"

    extract_stg_leitos(file_path, sep).pipe(treat_stg_leitos).pipe(load_stg_leitos, con, schema, tb_name)
