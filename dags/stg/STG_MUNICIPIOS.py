import DW_TOOLS as dwt
from pysus.online_data.SIM import get_municipios


@dwt.cronometrar
def extract_stg_municipios():
    return get_municipios()


@dwt.cronometrar
def treat_stg_municipios(df):
    df.columns = [col.lower() for col in df.columns]
    df.replace('', None, inplace=True)

    return df


def load_stg_municipios(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_stg_municipios(con, schema, tb_name, chunck=10000):
    dwt.truncate_table(con, schema, tb_name)

    extract_stg_municipios().pipe(treat_stg_municipios).pipe(load_stg_municipios, con, schema, tb_name, chunck)
