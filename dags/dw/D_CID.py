import pandas as pd
import datetime as dt
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_d_cid(con, schema, tb_name):
    query = f"""
        SELECT 
            NULLIF(TRIM(sub.subcat), '') AS cd_cid,
            NULLIF(TRIM(sub.descricao), '') AS ds_subcategoria,
            NULLIF(TRIM(cat.descricao), '') AS ds_categoria,
            NULLIF(TRIM(cap.descricao), '') AS ds_capitulo,
            NULLIF(TRIM(sub.restrsexo), '') AS fl_restricao_sexo,
            NOW() AS dt_carga
        FROM stg.stg_cid10_subcategorias sub
        INNER JOIN stg.stg_cid10_categorias cat
            ON (LEFT(TRIM(sub.subcat),3) = TRIM(cat.cat))
        INNER JOIN stg.stg_cid10_capitulos cap
            ON (TRIM(cat.cat) = TRIM(cap.codigo))
        LEFT JOIN {schema}.{tb_name} dim
            ON (TRIM(sub.subcat) = dim.cd_cid)
        WHERE dim.cd_cid IS NULL
    """

    return dwt.read_table_from_sql(query, con)


def treat_d_cid(df, max_sk):
    df.insert(0, "sk_cid", range(max(1, max_sk), len(df) + max(1, max_sk)))
    
    if not max_sk:
        dt_default = dt.datetime(1900, 1, 1)

        df_aux = pd.DataFrame(data = [
            [-1, -1, 'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado', dt_default],
            [-2, -2, 'Não Aplicável', 'Não Aplicável', 'Não Aplicável', 'Não Aplicável', dt_default],
            [-3, -3, 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido', dt_default]
        ], columns=df.columns)

        df = pd.concat([df_aux, df])

    fillna = {  
        "ds_subcategoria": 'Não Informado',
        "ds_categoria": 'Não Informado',
        "ds_capitulo": 'Não Informado',
        "fl_restricao_sexo": 'N'
    }

    return df.fillna(fillna)


def load_d_cid(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_d_cid(con, schema, tb_name, chunck=10000):
    tbl_extract = extract_d_cid(con, schema, tb_name)

    if not tbl_extract.empty:
        dim_size = dwt.get_dimension_size(con, schema, tb_name)
        treat_d_cid(tbl_extract, dim_size).pipe(load_d_cid, con, schema, tb_name, chunck)
