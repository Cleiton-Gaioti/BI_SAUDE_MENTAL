import pandas as pd
import datetime as dt
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_d_raca_cor(con, schema, tb_name):
    query = f"""
        SELECT DISTINCT
            NULLIF(TRIM(st.racacor), '') AS cd_raca_cor,
            CASE NULLIF(TRIM(st.racacor), '')::INTEGER
                WHEN 1 THEN 'Branca'
                WHEN 2 THEN 'Preta'
                WHEN 3 THEN 'Amarela'
                WHEN 4 THEN 'Parda'
                WHEN 5 THEN 'Indígena'
                ELSE NULL
            END AS ds_raca_cor,
            NOW() AS dt_carga
        FROM stg.stg_sim st
        LEFT JOIN {schema}.{tb_name} dim
            ON (NULLIF(TRIM(st.racacor), '')::INTEGER = dim.cd_raca_cor)
        WHERE NULLIF(TRIM(st.racacor), '') IS NOT NULL AND dim.cd_raca_cor IS NULL
    """

    return dwt.read_table_from_sql(query, con)


def treat_d_raca_cor(df, max_sk):
    df = df.astype({"cd_raca_cor": "Int64"})

    df.insert(0, "sk_raca_cor", range(max(1, max_sk), len(df) + max(1, max_sk)))
    
    if not max_sk:
        dt_default = dt.datetime(1900, 1, 1)

        df_aux = pd.DataFrame(data = [
            [-1, -1, 'Não Informado', dt_default],
            [-2, -2, 'Não Aplicável', dt_default],
            [-3, -3, 'Desconhecido', dt_default]
        ], columns=df.columns)

        df = pd.concat([df_aux, df])

    return df


def load_d_raca_cor(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_d_raca_cor(con, schema, tb_name, chunck=10000):
    tbl_extract = extract_d_raca_cor(con, schema, tb_name)

    if not tbl_extract.empty:
        dim_size = dwt.get_dimension_size(con, schema, tb_name)
        treat_d_raca_cor(tbl_extract, dim_size).pipe(load_d_raca_cor, con, schema, tb_name, chunck)
