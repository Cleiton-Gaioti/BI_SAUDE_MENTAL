import pandas as pd
import DW_TOOLS as dwt


def extract_cid10():
    url = "https://github.com/bigdata-icict/ETL-Dataiku-DSS/blob/master/SIM/cid10_tabela_capitulos.csv?raw=true"
    df_capitulos = pd.read_csv(url, sep=';')

    url = "https://github.com/bigdata-icict/ETL-Dataiku-DSS/blob/master/SIM/cid10_tabela_grupos.csv?raw=true"
    df_grupos = pd.read_csv(url, sep=';')

    url = "https://github.com/bigdata-icict/ETL-Dataiku-DSS/blob/master/SIM/CID-10-CATEGORIAS.CSV.utf8?raw=true"
    df_categorias = pd.read_csv(url, sep=';')

    url = "https://github.com/bigdata-icict/ETL-Dataiku-DSS/blob/master/SIM/CID-10-SUBCATEGORIAS.CSV.utf8?raw=true"
    df_subcategorias = pd.read_csv(url, sep=';')

    return df_capitulos, df_grupos, df_categorias, df_subcategorias


@dwt.cronometrar
def treat_cid10(df_capitulos, df_grupos, df_categorias, df_subcategorias):
    cols_names = {"codigo": "cd_cid", "descricao": "no_capitulo", "descricao_breve": "ds_capitulo"}
    df_capitulos.rename(columns=cols_names, inplace=True)

    cols_names = {"codigo": "cd_cid", "descricao": "no_grupo", "descricao_breve": "ds_grupo"}
    df_grupos.rename(columns=cols_names, inplace=True)

    cols_names = {"CAT": "cd_cid", "CLASSIF": "ds_classificacao", "DESCRICAO": "ds_categoria", "DESCRABREV": "ds_abreviada"}
    df_categorias = df_categorias.filter(cols_names.keys()).rename(columns=cols_names)

    cols_names = {"SUBCAT": "cd_cid", "RESTRSEXO": "ds_restricao_sexo", "CAUSAOBITO": "ds_causa_obito", "DESCRICAO": "ds_subcategoria", "DESCRABREV": "ds_abreviada"}
    df_subcategorias = df_subcategorias.filter(cols_names.keys()).rename(columns=cols_names)

    df = pd.merge(
        left=df_capitulos,
        right=df_grupos,
        how='left',
        on='cd_cid'
    ).merge(
        right=df_categorias,
        how='left',
        on='cd_cid'
    ).merge(
        right=df_subcategorias,
        how='left',
        on='cd_cid'
    )

    return df


def load_cid10(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='replace', index=False, chunksize=chunck, method='multi')


def run_cid10(con, schema, tb_name, chunck=10000):
    df_capitulos, df_grupos, df_categorias, df_subcategorias = extract_cid10()
    
    treat_cid10(df_capitulos, df_grupos, df_categorias, df_subcategorias).pipe(load_cid10, con, schema, tb_name, chunck)
