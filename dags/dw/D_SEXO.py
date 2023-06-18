import pandas as pd
import datetime as dt
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_d_sexo(con, schema, tb_name):
    query = f"""
        WITH sexo AS (
            SELECT DISTINCT
                CASE
                    WHEN NULLIF(TRIM(sim.sexo), '')::INTEGER NOT IN (1,2) THEN -1
                    ELSE NULLIF(TRIM(sim.sexo), '')::INTEGER
                END AS cd_sexo,
                CASE NULLIF(TRIM(sim.sexo), '')::INTEGER
                    WHEN -1 THEN 'Não Informado'
                    WHEN 1  THEN 'Masculino'
                    WHEN 2  THEN 'Feminino'
                END AS ds_sexo,
                NOW() AS dt_carga
            FROM stg.stg_sim sim
        )
        SELECT stg.*
        FROM SEXO stg
        LEFT JOIN {schema}.d_sexo ds
            ON (stg.cd_sexo = ds.cd_sexo)
        WHERE ds.cd_sexo IS NULL
    """

    return dwt.read_table_from_sql(query, con)


def treat_d_sexo(df, max_sk):
    df = df.astype({"cd_sexo": "Int64"})

    df.insert(0, "sk_sexo", range(max(1, max_sk), len(df) + max(1, max_sk)))
    
    if not max_sk:
        dt_default = dt.datetime(1900, 1, 1)

        df_aux = pd.DataFrame(data = [
            [-1, -1, 'Não Informado', dt_default],
            [-2, -2, 'Não Aplicável', dt_default],
            [-3, -3, 'Desconhecido', dt_default]
        ], columns=df.columns)

        df = pd.concat([df_aux, df]).drop_duplicates(["cd_sexo", "ds_sexo"])

    return df


def load_d_sexo(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_d_sexo(con, schema, tb_name, chunck=10000):
    tbl_extract = extract_d_sexo(con, schema, tb_name)

    if not tbl_extract.empty:
        dim_size = dwt.get_dimension_size(con, schema, tb_name)
        treat_d_sexo(tbl_extract, dim_size).pipe(load_d_sexo, con, schema, tb_name, chunck)
