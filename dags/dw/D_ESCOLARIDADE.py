import pandas as pd
import datetime as dt
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_d_escolaridade(con, schema, tb_name):
    query = f"""
        WITH escolaridade AS (
            SELECT
                NULLIF(TRIM(esc), '')::INTEGER AS cd_escolaridade,
                NULLIF(TRIM(seriescfal), '')::INTEGER AS nu_serie
            FROM stg.stg_sim
            WHERE esc IS NOT NULL
            UNION
            SELECT
                NULLIF(TRIM(escmae), '')::INTEGER,
                NULLIF(TRIM(seriescmae), '')::INTEGER
            FROM stg.stg_sim
            WHERE escmae IS NOT NULL
        )
        SELECT DISTINCT
            CASE esc.cd_escolaridade
                WHEN 9 THEN 0
                ELSE esc.cd_escolaridade
            END AS cd_escolaridade,
            CASE esc.cd_escolaridade
                WHEN 0 THEN 'Ignorado'
                WHEN 1 THEN 'Nenhuma'
                WHEN 2 THEN '1 a 3 anos'
                WHEN 3 THEN '4 a 7 anos'
                WHEN 4 THEN '8 a 11 anos'
                WHEN 5 THEN '12 anos ou mais'
                WHEN 9 THEN 'Ignorado'
                ELSE NULL
            END ds_escolaridade,
            esc.nu_serie,
            NOW() AS dt_carga
        FROM escolaridade esc
        LEFT JOIN {schema}.{tb_name} dim
            ON (esc.cd_escolaridade = dim.cd_escolaridade AND esc.nu_serie = dim.nu_serie)
        WHERE esc.cd_escolaridade IS NOT NULL AND dim.cd_escolaridade IS NULL
    """

    return dwt.read_table_from_sql(query, con)


def treat_d_escolaridade(df, max_sk):
    df = df.astype({"cd_escolaridade": "Int64", "nu_serie": "Int64"})

    df.insert(0, "sk_escolaridade", range(max(1, max_sk), len(df) + max(1, max_sk)))
    
    if not max_sk:
        dt_default = dt.datetime(1900, 1, 1)

        df_aux = pd.DataFrame(data = [
            [-1, -1, 'Não Informado', -1, dt_default],
            [-2, -2, 'Não Aplicável', -2, dt_default],
            [-3, -3, 'Desconhecido', -3, dt_default]
        ], columns=df.columns)

        df = pd.concat([df_aux, df])
    
    fillna = {'ds_escolaridade': 'Não Informado', 'nu_serie': -3}

    return df.fillna(fillna)


def load_d_escolaridade(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_d_escolaridade(con, schema, tb_name, chunck=10000):
    tbl_extract = extract_d_escolaridade(con, schema, tb_name)

    if not tbl_extract.empty:
        dim_size = dwt.get_dimension_size(con, schema, tb_name)
        treat_d_escolaridade(tbl_extract, dim_size).pipe(load_d_escolaridade, con, schema, tb_name, chunck)
