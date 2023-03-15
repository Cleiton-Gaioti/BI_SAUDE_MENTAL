import pandas as pd
import datetime as dt
import DW_TOOLS as dwt


def extract_d_naturalidade(con, schema, tb_name):
    query = f"""
        WITH naturalidade AS (
            SELECT DISTINCT
                sp."pais-m49" AS cd_naturalidade,
                sp."pais-nome" AS ds_naturalidade,
                'PAÍS' AS ds_nivel
            FROM stg.stg_pais sp
            LEFT JOIN stg.stg_uf su
                ON (sp."pais-m49" = (su."uf-id" + 800))
            WHERE su."uf-id" IS NULL
            UNION
            SELECT DISTINCT
                (su."uf-id" + 800) AS cd_naturalidade,
                su."uf-nome" AS ds_naturalidade,
                'UF-BRASIL' AS ds_nivel
            FROM stg.stg_uf su
        )
        SELECT
            nat.cd_naturalidade,
            nat.ds_naturalidade,
            nat.ds_nivel,
            NOW() as dt_carga
        FROM naturalidade nat
        LEFT JOIN {schema}.{tb_name} dn
            ON (nat.cd_naturalidade = dn.cd_naturalidade)
        WHERE dn.cd_naturalidade IS NULL
    """

    df = dwt.read_table_from_sql(query, con)

    return df


def treat_d_naturalidade(df, max_sk):
    df = df.astype({"cd_naturalidade": "Int64"})

    df.insert(0, "sk_naturalidade", range(max(1, max_sk), len(df) + max(1, max_sk)))
    
    if not max_sk:
        dt_default = dt.datetime(1900, 1, 1)

        df_aux = pd.DataFrame(data = [
            [-1, -1, 'Não Informado', 'Não Informado', dt_default],
            [-2, -2, 'Não Aplicável', 'Não Aplicável', dt_default],
            [-3, -3, 'Desconhecido', 'Desconhecido', dt_default]
        ], columns=df.columns)

        df = pd.concat([df_aux, df])

    return df


def load_d_naturalidade(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_d_naturalidade(con, schema, tb_name, chunck=10000):
    tbl_extract = extract_d_naturalidade(con, schema, tb_name)

    if not tbl_extract.empty:
        dim_size = dwt.get_dimension_size(con, schema, tb_name)
        treat_d_naturalidade(tbl_extract, dim_size).pipe(load_d_naturalidade, con, schema, tb_name, chunck)
