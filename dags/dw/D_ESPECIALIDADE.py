import pandas as pd
import datetime as dt
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_d_especialidade(con, schema, tb_name):
    query = f"""
        SELECT DISTINCT
            sih.espec::INTEGER AS cd_especialidade
            , UPPER(TRIM(value)) AS ds_especialidade
            , NOW() AS dt_carga
        FROM stg.stg_sih sih
        INNER JOIN stg.stg_leitos sl
            ON (sih.espec::INTEGER = sl.cod)
        LEFT JOIN {schema}.{tb_name} dim
            ON (sih.espec::INTEGER = dim.cd_especialidade)
        WHERE dim.cd_especialidade IS NULL
    """

    return dwt.read_table_from_sql(query, con)


def treat_d_especialidade(df, max_sk):
    df.insert(0, "sk_especialidade", range(max(1, max_sk), len(df) + max(1, max_sk)))

    if not max_sk:
        dt_default = dt.datetime(1900, 1, 1)

        df_aux = pd.DataFrame(data=[
            [-1, -1, 'Não Informado', dt_default],
            [-2, -2, 'Não Aplicável', dt_default],
            [-3, -3, 'Desconhecido', dt_default]
        ], columns=df.columns)

        df = pd.concat([df_aux, df])

    return df


def load_d_especialidade(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_d_especialidade(con, schema, tb_name, chunck=10000):
    tbl_extract = extract_d_especialidade(con, schema, tb_name)

    if not tbl_extract.empty:
        dim_size = dwt.get_dimension_size(con, schema, tb_name)
        treat_d_especialidade(tbl_extract, dim_size).pipe(load_d_especialidade, con, schema, tb_name, chunck)
