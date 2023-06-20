import pandas as pd
import datetime as dt
import DW_TOOLS as dwt


def extract_f_obito(con, schema, tb_name, start_year, end_year):
    query = f"""
        WITH sim AS (
            SELECT
                NULLIF(TRIM(sim.codmunnatu), '')::INTEGER AS cd_municipio_naturalidade,
                NULLIF(TRIM(sim.codmunocor), '')::INTEGER AS cd_municipio_ocorrencia,
                NULLIF(TRIM(sim.codmunres), '')::INTEGER AS cd_municipio_residencia,
                NULLIF(TRIM(sim.ocup), '')::INTEGER AS cd_ocupacao,
                NULLIF(TRIM(sim."natural"), '')::INTEGER AS cd_naturalidade,
                NULLIF(TRIM(sim.racacor), '')::INTEGER AS cd_raca_cor,
                COALESCE(NULLIF(TRIM(sim.causabas), ''), 'Não Informado') AS cd_cid,
                NULLIF(TRIM(sim.sexo), '')::INTEGER AS cd_sexo,
                COALESCE(NULLIF(TRIM(sim.contador), '')::INTEGER, -2) AS cd_registro,
                COALESCE(REGEXP_REPLACE(NULLIF(TRIM(sim.crm), ''), '[^0-9]*', '', 'g'), 'Não Informado') AS nu_crm_medico_atestante,
                COALESCE(TO_DATE(NULLIF(TRIM(sim.dtcadastro), ''), 'DDMMYY'), TO_DATE('01011900', 'DDMMYY')) AS dt_cadastro,
                COALESCE(TO_DATE(NULLIF(TRIM(sim.dtatestado), ''), 'DDMMYY'), TO_DATE('01011900', 'DDMMYY')) AS dt_atestado,
                CASE
                    WHEN
                        TO_DATE(NULLIF(TRIM(sim.dtobito), ''), 'DDMMYY') >= TO_DATE('0101{start_year}', 'DDMMYY')
                            AND TO_DATE(NULLIF(TRIM(sim.dtobito), ''), 'DDMMYY') < TO_DATE('0101{end_year}', 'DDMMYY')
                    THEN TO_DATE(NULLIF(TRIM(sim.dtobito), ''), 'DDMMYY')
                    ELSE TO_DATE('01011900', 'DDMMYY')
                END AS dt_obito,
                COALESCE(TO_DATE(NULLIF(TRIM(sim.dtnasc), ''), 'DDMMYY'), TO_DATE('01011900', 'DDMMYY')) AS dt_nascimento_falecido,
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
                CASE
                    WHEN LEFT(NULLIF(TRIM(sim.idade), ''),1) IN ('1','2','3')
                        THEN 0
                    WHEN LEFT(NULLIF(TRIM(sim.idade), ''),1) = '4'
                        THEN RIGHT(NULLIF(TRIM(sim.idade), ''),2)::INTEGER
                    WHEN LEFT(NULLIF(TRIM(sim.idade), ''),1) = '5'
                        THEN (100 + RIGHT(NULLIF(TRIM(sim.idade), ''),2)::INTEGER)
                    ELSE NULL
                END AS vl_idade_falecido
            FROM stg.stg_sim sim
        )
        SELECT DISTINCT
              cid.sk_cid AS sk_cid_causa_obito
            , mun_ocor.sk_municipio AS sk_municipio_ocorrencia_obito
            , mun_nat.sk_municipio AS sk_municipio_naturalidade_falecido
            , mun_res.sk_municipio AS sk_municipio_residencia_falecido
            , nat.sk_naturalidade
            , ocup.sk_ocupacao AS sk_ocupacao_falecido
            , raca.sk_raca_cor
            , sexo.sk_sexo
            , stg.cd_registro
            , stg.cd_cid AS nu_cid
            , stg.ds_assistencia_medica
            , stg.ds_atestante
            , stg.ds_circunstancia_obito
            , stg.nu_crm_medico_atestante
            , stg.dt_cadastro
            , stg.dt_atestado
            , stg.dt_obito
            , stg.dt_nascimento_falecido
            , stg.vl_idade_falecido
            , NOW() AS dt_carga
        FROM sim stg
        LEFT JOIN {schema}.{tb_name} fato
            ON (stg.cd_registro = fato.cd_registro AND stg.dt_obito = fato.dt_obito AND stg.cd_cid = fato.nu_cid AND stg.nu_crm_medico_atestante = fato.nu_crm_medico_atestante)
        LEFT JOIN {schema}.d_cid cid
            ON (stg.cd_cid = cid.cd_cid)
        LEFT JOIN {schema}.d_municipio mun_ocor
            ON (stg.cd_municipio_ocorrencia = mun_ocor.cd_municipio_normalizado)
        LEFT JOIN {schema}.d_municipio mun_nat
            ON (stg.cd_municipio_naturalidade = mun_nat.cd_municipio_normalizado)
        LEFT JOIN {schema}.d_municipio mun_res
            ON (stg.cd_municipio_residencia = mun_res.cd_municipio_normalizado)
        LEFT JOIN {schema}.d_naturalidade nat
            ON (stg.cd_naturalidade = nat.cd_naturalidade)
        LEFT JOIN {schema}.d_ocupacao ocup
            ON (stg.cd_ocupacao = ocup.cd_ocupacao)
        LEFT JOIN {schema}.d_raca_cor raca
            ON (stg.cd_raca_cor = raca.cd_raca_cor)
        LEFT JOIN {schema}.d_sexo sexo
            ON (stg.cd_sexo = sexo.cd_sexo)
        WHERE fato.cd_registro IS NULL
    """

    return dwt.read_sql_query(query, con)


def treat_f_obito(list_values, columns):
    dtypes = {
        'sk_municipio_naturalidade_falecido': 'Int64',
        'sk_municipio_ocorrencia_obito': 'Int64',
        'sk_municipio_residencia_falecido': 'Int64',
        'sk_ocupacao_falecido': 'Int64',
        'sk_naturalidade': 'Int64',
        'sk_raca_cor': 'Int64',
        'sk_cid_causa_obito': 'Int64',
        'sk_sexo': 'Int64',
        'cd_registro': 'Int64',
        'vl_idade_falecido': 'Int64'
    }

    fillna = {
        'sk_municipio_naturalidade_falecido': -2,
        'sk_municipio_ocorrencia_obito': -2,
        'sk_municipio_residencia_falecido': -2,
        'sk_ocupacao_falecido': -2,
        'sk_naturalidade': -2,
        'sk_raca_cor': -2,
        'sk_cid_causa_obito': -2,
        'sk_sexo': -1,
        'ds_assistencia_medica': 'Não Informado',
        'ds_atestante': 'Não Informado',
        'ds_circunstancia_obito': 'Não Informado',
        'vl_idade_falecido': -2
    }

    return pd.DataFrame(data=list_values)[columns].astype(dtypes).fillna(fillna)


def load_f_obito(df, con, schema, tb_name, columns):
    dwt.load_with_csv(df, con, schema, tb_name, columns)


def run_f_obito(con, schema, tb_name, start_year, end_year=0, chunck=10000):
    columns = dwt.get_table_columns(con, schema, tb_name)

    end_year = max(start_year, end_year) + 1

    result = extract_f_obito(con, schema, tb_name, start_year, end_year)

    while True:
        list_values = result.fetchmany(chunck)

        if not list_values:
            break
    
        treat_f_obito(list_values, columns).pipe(load_f_obito, con, schema, tb_name, columns)
