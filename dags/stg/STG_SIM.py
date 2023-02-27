import numpy as np
import pandas as pd
from DW_TOOLS import cronometrar
from pysus.online_data.SIM import download


@cronometrar
def extract_sim(uf, start_year, end_year):
    df = pd.concat([download(uf, year) for year in range(start_year, end_year + 1)])

    return df


@cronometrar
def treat_sim(df):
    df = df.astype(str).replace({'': None, '<NA>': None, np.nan: None, 'nan': None})

    df.columns = [col_name.lower() for col_name in df.columns]

    if 'dtobito' in df.columns:
        df['dtobito'] = pd.to_datetime(df['dtobito'], format='%d%m%Y').rename({'dtobito': 'dt_obito'})

    if 'dtnasc' in df.columns:
        df['dtnasc'] = pd.to_datetime(df['dtnasc'], format='%d%m%Y')

    if 'idade' in df.columns:
        df.loc[(df['idade'] == '000') | (df['idade'] == '999'), 'idade'] = None
        
        df['unidade_idade'] = df['idade'].apply(lambda x: x[0] if x else x).astype('Int64')
        df['qtd_idade'] = df['idade'].apply(lambda x: x[1:] if x else x).astype('Int64')

    if 'dtatestado' in df.columns:
        df['dtatestado'] = pd.to_datetime(df['dtatestado'], format='%d%m%Y')

    if 'dtinvestig' in df.columns:
        df['dtinvestig'] = pd.to_datetime(df['dtinvestig'], format='%d%m%Y')

    if 'dtcadastro' in df.columns:
        df['dtcadastro'] = pd.to_datetime(df['dtcadastro'], format='%d%m%Y')

    if 'dtrecebim' in df.columns:
        df['dtrecebim'] = pd.to_datetime(df['dtrecebim'], format='%d%m%Y')

    if 'dtrecoriga' in df.columns:
        df['dtrecoriga'] = pd.to_datetime(df['dtrecoriga'], format='%d%m%Y')

    return df


def load_sim(df, con, schema, tb_name):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='replace', index=False, chunksize=10000, method='multi')


def run_sim(uf, start_year, end_year, con, schema, tb_name):
    extract_sim(uf, start_year, end_year).pipe(treat_sim).pipe(load_sim, con, schema, tb_name)
