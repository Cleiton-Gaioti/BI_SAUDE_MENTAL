import pandas as pd
import DW_TOOLS as dwt
from ibge.localidades import Estados
from pysus.online_data.SIM import download


def extract_sim(uf, year):
    path = download(uf, year)
    
    if path:
        return pd.read_parquet(path)
    else:
        return None


@dwt.cronometrar
def treat_sim(df, columns):
    df.columns = [col_name.lower() for col_name in df.columns]

    for col in columns:
        if col not in df.columns:
            df.insert(0, col, None)

    df = df.astype('object').apply(lambda x: x.str.strip()).replace('', None)

    return df[columns]


def load_sim(df, con, schema, tb_name, columns):
    dwt.load_with_csv(df, con, schema, tb_name, columns)


def run_sim(ufs, start_year, con, schema, tb_name, end_year=0):
    dwt.truncate_table(con, schema, tb_name)
    columns = dwt.get_table_columns(con, schema, tb_name)

    end_year = max(start_year, end_year) + 1

    if isinstance(ufs, str):
        if ufs.lower() == 'all':
            ufs = Estados().getSigla()
        else:
            ufs = [ufs]

    years = list(range(start_year, end_year))

    for uf in ufs:
        for year in years:
            tbl_extract = extract_sim(uf, year)

            if isinstance(tbl_extract, pd.DataFrame):
                treat_sim(tbl_extract, columns).pipe(load_sim, con, schema, tb_name, columns)