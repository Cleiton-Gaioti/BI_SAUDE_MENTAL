import pandas as pd
import datetime as dt
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_f_obito(con, schema, tb_name):
    query = f"""
        SELECT DISTINCT
            NULLIF(TRIM(sim.codmunnatu), '')::INTEGER AS cd_municipio_naturalidade,
            NULLIF(TRIM(sim.codmunocor), '')::INTEGER AS cd_municipio_ocorrencia,
            NULLIF(TRIM(sim.codmunres), '')::INTEGER AS cd_municipio_residencia,
            NULLIF(TRIM(sim.ocup), '')::INTEGER AS cd_ocupacao,
            NULLIF(TRIM(sim."natural"), '')::INTEGER AS cd_naturalidade,
            NULLIF(TRIM(sim.racacor), '')::INTEGER AS cd_raca_cor,
            NULLIF(TRIM(sim.causabas), '') AS cd_cid,
            NULLIF(TRIM(sim.esc), '')::INTEGER AS cd_escolaridade,
            NULLIF(TRIM(sim.seriescfal), '')::INTEGER AS nu_serie,
            NULLIF(TRIM(sim.sexo), '')::INTEGER AS cd_sexo,
            NULLIF(TRIM(sim.contador), '')::INTEGER AS cd_registro,
            CASE NULLIF(TRIM(sim.assistmed), '')
                WHEN '1' THEN 'Com assistência'
                WHEN '2' THEN 'Sem assistência'
                WHEN '9' THEN 'Igorado'
                ELSE NULL
            END AS ds_assistencia_medica,
            CASE NULLIF(TRIM(sim.atestante), '')
                WHEN '1' THEN 'Sim'
                WHEN '2' THEN 'Substituto'
                WHEN '3' THEN 'IML'
                WHEN '4' THEN 'SVO'
                WHEN '5' THEN 'Outros'
                ELSE NULL
            END AS ds_atestante,
            CASE NULLIF(TRIM(sim.circobito), '')
                WHEN '1' THEN 'Acidente'
                WHEN '2' THEN 'Suicídio'
                WHEN '3' THEN 'Homicídio'
                WHEN '4' THEN 'Outros'
                WHEN '9' THEN 'Ignorado'
                ELSE NULL
            END AS ds_circunstancia_obito,
            REGEXP_REPLACE(NULLIF(TRIM(sim.crm), ''), '[^0-9]*', '', 'g') AS nu_crm_medico_atestante,
            TO_DATE(NULLIF(TRIM(sim.dtcadastro), ''), 'DDMMYY') AS dt_cadastro,
            TO_DATE(NULLIF(TRIM(sim.dtatestado), ''), 'DDMMYY') AS dt_atestado,
            TO_DATE(NULLIF(TRIM(sim.dtobito), ''), 'DDMMYY') AS dt_obito,
            TO_DATE(NULLIF(TRIM(sim.dtnasc), ''), 'DDMMYY') AS dt_nascimento_falecido,
            CASE
                WHEN NULLIF(TRIM(sim.idade), '') = '000'
                    THEN -2
                WHEN LEFT(NULLIF(TRIM(sim.idade), ''),1) IN ('0','1','2','3','4')
                    THEN RIGHT(NULLIF(TRIM(sim.idade), ''),2)::INTEGER
                WHEN LEFT(NULLIF(TRIM(sim.idade), ''),1) = '5'
                    THEN (100 + RIGHT(NULLIF(TRIM(sim.idade), ''),2)::INTEGER)
                ELSE -2
            END AS vl_idade_falecido,
            CASE
                WHEN NULLIF(TRIM(sim.idade), '') = '000' THEN 'Não Informado'
                ELSE
                    CASE LEFT(NULLIF(TRIM(sim.idade), ''),1)
                        WHEN '0' THEN 'MINUTOS'
                        WHEN '1' THEN 'HORAS'
                        WHEN '2' THEN 'DIAS'
                        WHEN '3' THEN 'MESES'
                        WHEN '4' THEN 'ANOS'
                        WHEN '5' THEN 'ANOS'
                        ELSE 'Não Informado'
                    END
            END AS ds_unidade_idade_falecido,
            NOW() as dt_carga
        FROM stg.stg_sim sim
        LEFT JOIN {schema}.{tb_name} fato
            ON (NULLIF(TRIM(sim.contador), '')::INTEGER = fato.cd_registro)
        WHERE fato.cd_registro IS NULL
    """

    tbl_fato = dwt.read_table_from_sql(query, con)

    if not tbl_fato.empty:
        with con.connect() as conn:
            d_cid = pd.read_sql_table('d_cid', conn, schema, columns=['sk_cid', 'cd_cid'])

            d_escolaridade = pd.read_sql_table('d_escolaridade', conn, schema, columns=['sk_escolaridade', 'cd_escolaridade', 'nu_serie'])

            d_municipio = pd.read_sql_table('d_municipio', conn, schema, columns=['sk_municipio', 'cd_municipio_normalizado'])

            d_naturalidade = pd.read_sql_table('d_naturalidade', conn, schema, columns=['sk_naturalidade', 'cd_naturalidade'])

            d_ocupacao = pd.read_sql_table('d_ocupacao', conn, schema, columns=['sk_ocupacao', 'cd_ocupacao'])

            d_raca_cor = pd.read_sql_table('d_raca_cor', conn, schema, columns=['sk_raca_cor', 'cd_raca_cor'])

            d_sexo = pd.read_sql_table('d_sexo', conn, schema, columns=['sk_sexo', 'cd_sexo'])

        tbl_fato = pd.merge(
            left=tbl_fato,
            right=d_cid,
            how='left',
            on='cd_cid'
        ).merge(
            right=d_escolaridade,
            how='left',
            on=['cd_escolaridade', 'nu_serie']
        ).merge(
            right=d_municipio,
            how='left',
            left_on='cd_municipio_ocorrencia',
            right_on='cd_municipio_normalizado',
        ).merge(
            right=d_municipio,
            how='left',
            left_on='cd_municipio_naturalidade',
            right_on='cd_municipio_normalizado',
            suffixes=['', '_naturalidade_falecido']
        ).merge(
            right=d_municipio,
            how='left',
            left_on='cd_municipio_residencia',
            right_on='cd_municipio_normalizado',
            suffixes=['', '_residencia_falecido'] 
        ).merge(
            right=d_naturalidade,
            how='left',
            on='cd_naturalidade'
        ).merge(
            right=d_ocupacao,
            how='left',
            on='cd_ocupacao'
        ).merge(
            right=d_raca_cor,
            how='left',
            on='cd_raca_cor'
        ).merge(
            right=d_sexo,
            how='left',
            on='cd_sexo')

    return tbl_fato


def treat_f_obito(df, columns):
    col_names = {
        'sk_cid': 'sk_cid_causa_obito',
        'sk_escolaridade': 'sk_escolaridade_falecido',
        'sk_municipio': 'sk_municipio_ocorrencia_obito', 
        'sk_ocupacao': 'sk_ocupacao_falecido'
    }

    dtypes = {
        'sk_municipio_naturalidade_falecido': 'Int64',
        'sk_municipio_ocorrencia_obito': 'Int64',
        'sk_municipio_residencia_falecido': 'Int64',
        'sk_ocupacao_falecido': 'Int64',
        'sk_naturalidade': 'Int64',
        'sk_raca_cor': 'Int64',
        'sk_cid_causa_obito': 'Int64',
        'sk_escolaridade_falecido': 'Int64',
        'sk_sexo': 'Int64',
        'cd_registro': 'Int64',
        'vl_idade_falecido': 'Int64',
    }

    fillna = {
        'sk_municipio_naturalidade_falecido': -2,
        'sk_municipio_ocorrencia_obito': -2,
        'sk_municipio_residencia_falecido': -2,
        'sk_ocupacao_falecido': -2,
        'sk_naturalidade': -2,
        'sk_raca_cor': -2,
        'sk_cid_causa_obito': -2,
        'sk_escolaridade_falecido': -2,
        'sk_sexo': -2,
        'cd_registro': -2,
        'ds_assistencia_medica': 'Não informado',
        'ds_atestante': 'Não informado',
        'ds_circunstancia_obito': 'Não informado',
        'nu_crm_medico_atestante': 'Não informado',
        'dt_cadastro': pd.to_datetime('01/01/1900'),
        'dt_atestado': pd.to_datetime('01/01/1900'),
        'dt_obito': pd.to_datetime('01/01/1900'),
        'dt_nascimento_falecido': pd.to_datetime('01/01/1900'),
        'vl_idade_falecido': -2,
        'ds_unidade_idade_falecido': 'Não informado'
    }

    df = df.rename(columns=col_names)[columns].astype(dtypes).fillna(fillna)

    return df


def load_f_obito(df, con, schema, tb_name, columns):
    dwt.load_with_csv(df, con, schema, tb_name, columns)


def run_f_obito(con, schema, tb_name):
    tbl_extract = extract_f_obito(con, schema, tb_name)

    if not tbl_extract.empty:
        columns = dwt.get_table_columns(con, schema, tb_name)
        
        treat_f_obito(tbl_extract, columns).pipe(load_f_obito, con, schema, tb_name, columns)
