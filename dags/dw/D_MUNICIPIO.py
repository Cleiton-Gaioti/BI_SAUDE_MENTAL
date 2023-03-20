import pandas as pd
import datetime as dt
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_d_municipio(con, schema, tb_name):
    query = f"""
        SELECT DISTINCT
            st."municipio-id" AS cd_municipio
            , LEFT(st."municipio-id"::TEXT,6) AS cd_municipio_normalizado
            , NULLIF(TRIM(st."municipio-nome"), '') AS no_municipio
            , st."microrregiao-id" AS cd_microrregiao
            , NULLIF(TRIM(st."microrregiao-nome"), '') AS no_microrregiao
            , st."mesorregiao-id" AS cd_mesorregiao
            , NULLIF(TRIM(st."mesorregiao-nome"), '') AS no_mesorregiao
            , st."regiao-imediata-id" AS cd_regiao_imediata
            , NULLIF(TRIM(st."regiao-imediata-nome"), '') AS no_regiao_imediata
            , st."regiao-intermediaria-id" AS cd_regiao_intermediaria
            , NULLIF(TRIM(st."regiao-intermediaria-nome"), '') AS no_regiao_intermediaria
            , st."uf-id" AS cd_uf
            , NULLIF(TRIM(st."uf-sigla"), '') AS ds_sigla_uf
            , NULLIF(TRIM(st."uf-nome"), '') AS no_uf
            , st."regiao-id" AS cd_regiao
            , NULLIF(TRIM(st."regiao-sigla"), '') AS ds_sigla_regiao
            , NULLIF(TRIM(st."regiao-nome"), '') AS no_regiao
            , NOW() as dt_carga
        FROM stg.stg_municipios st
        LEFT JOIN {schema}.{tb_name} dim
            ON (st."municipio-id" = dim.cd_municipio)
        WHERE dim.cd_municipio IS NULL
    """

    df = dwt.read_table_from_sql(query, con)

    return df


@dwt.cronometrar
def treat_d_municipio(df, max_sk):
    dtypes = {
        "cd_municipio": "Int64",
        "cd_municipio_normalizado": "Int64",
        "cd_microrregiao": "Int64",
        "cd_mesorregiao": "Int64",
        "cd_regiao_imediata": "Int64",
        "cd_regiao_intermediaria": "Int64",
        "cd_uf": "Int64",
        "cd_regiao": "Int64",
        "dt_carga": "datetime64[ns]",
    }

    df = df.astype(dtypes)
    df.insert(0, "sk_municipio", range(max(1, max_sk), len(df) + max(1, max_sk)))
    
    if not max_sk:
        dt_default = dt.datetime(1900, 1, 1)

        df_aux = pd.DataFrame(data = [
            [-1, -1, -1, 'Não Informado', -1, 'Não Informado', -1, 'Não Informado', -1, 'Não Informado', -1,
             'Não Informado', -1, 'Não Informado', 'Não Informado', -1, 'Não Informado', 'Não Informado', dt_default],
            [-2, -2, -2, 'Não Aplicável', -2, 'Não Aplicável', -2, 'Não Aplicável', -2, 'Não Aplicável', -2,
             'Não Aplicável', -2, 'Não Aplicável', 'Não Aplicável', -2, 'Não Aplicável', 'Não Aplicável', dt_default],
            [-3, -3, -3, 'Desconhecido', -3, 'Desconhecido', -3, 'Desconhecido', -3, 'Desconhecido', -3,
             'Desconhecido', -3, 'Desconhecido', 'Desconhecido', -3, 'Desconhecido', 'Desconhecido', dt_default]
        ], columns=df.columns)

        df = pd.concat([df_aux, df])

    return df


def load_d_municipio(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_d_municipio(con, schema, tb_name, chunck=10000):
    tbl_extract = extract_d_municipio(con, schema, tb_name)

    if not tbl_extract.empty:
        dim_size = dwt.get_dimension_size(con, schema, tb_name)
        treat_d_municipio(tbl_extract, dim_size).pipe(load_d_municipio, con, schema, tb_name, chunck)