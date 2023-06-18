import pandas as pd
import datetime as dt
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_d_procedimento(con, schema, tb_name):
    query = f"""
        SELECT DISTINCT
            proc_rea::INTEGER AS cd_procedimento
            , UPPER(TRIM(value)) AS ds_procedimento
            , NOW() AS dt_carga
        FROM stg.stg_sih ss
        INNER JOIN stg.stg_tb_sigtab st
            ON (ss.proc_rea::INTEGER = st.cod)
        LEFT JOIN {schema}.{tb_name} dim
            ON (ss.proc_rea::INTEGER = dim.cd_procedimento)
        WHERE dim.cd_procedimento IS NULL
    """

    return dwt.read_table_from_sql(query, con)


def treat_d_procedimento(df, max_sk):
    df.insert(0, "sk_procedimento", range(max(1, max_sk), len(df) + max(1, max_sk)))

    if not max_sk:
        dt_default = dt.datetime(1900, 1, 1)

        df_aux = pd.DataFrame(data=[
            [-1, -1, 'Não Informado', dt_default],
            [-2, -2, 'Não Aplicável', dt_default],
            [-3, -3, 'Desconhecido', dt_default]
        ], columns=df.columns)

        df = pd.concat([df_aux, df])

    return df


def load_d_procedimento(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_d_procedimento(con, schema, tb_name, chunck=10000):
    tbl_extract = extract_d_procedimento(con, schema, tb_name)

    if not tbl_extract.empty:
        dim_size = dwt.get_dimension_size(con, schema, tb_name)
        treat_d_procedimento(tbl_extract, dim_size).pipe(load_d_procedimento, con, schema, tb_name, chunck)
