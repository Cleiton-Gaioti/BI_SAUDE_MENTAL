import pandas as pd
import datetime as dt
import DW_TOOLS as dwt


def extract_f_hospitalizacao(con, schema, tb_name):
    query = f"""
        WITH sih AS (
            SELECT
                NULLIF(TRIM(sih.munic_mov), '')::INTEGER AS cd_municipio_estabelecimento
                , NULLIF(TRIM(sih.munic_res), '')::INTEGER AS cd_municipio_residencia_paciente
                , NULLIF(TRIM(sih.cgc_hosp), '') AS nu_cnpj_estabelecimento
                , NULLIF(UPPER(TRIM(sih.diag_princ)), '') AS cd_cid_diagnostico_primario
                , NULLIF(UPPER(TRIM(sih.diag_secun)), '') AS cd_cid_diagnostico_secundario
                , CASE
                    WHEN NULLIF(TRIM(sih.sexo), '')::INTEGER = 1 THEN 1
                    WHEN NULLIF(TRIM(sih.sexo), '')::INTEGER IN (2,3) THEN 2
                    ELSE NULL
                END AS cd_sexo
                , CASE NULLIF(TRIM(sih.raca_cor), '')::INTEGER
                    WHEN 1 THEN 1
                    WHEN 2 THEN 2
                    WHEN 3 THEN 4
                    WHEN 4 THEN 3
                    WHEN 5 THEN 3
                    ELSE NULL
                END AS cd_raca_cor
                , NULLIF(TRIM(sih.cbor), '')::INTEGER AS cd_ocupacao
                , TRIM(sih.proc_solic)::INTEGER AS cd_procedimento_solicitado
                , TRIM(sih.proc_rea)::INTEGER AS cd_procedimento_realizado
                , TRIM(sih.espec)::INTEGER AS cd_especialidade_leito
                , TRIM(sih.mes_cmpt) || TRIM(sih.ano_cmpt) AS nu_competencia
                , TRIM(sih.n_aih) AS nu_aih
                , CASE sih.ident::INTEGER
                    WHEN 1 THEN 'NORMAL'
                    WHEN 5 THEN 'LONGA PERMANÊNCIA'
                    ELSE NULL
                END AS ds_tipo_aih
                , TO_DATE(NULLIF(UPPER(TRIM(sih.nasc)), ''), 'YYYYMMDD') AS dt_nascimento_paciente
                , sih.qt_diarias::INTEGER AS qtd_diarias_internacao
                , sih.val_tot::FLOAT AS vl_total_internacao
                , TO_DATE(NULLIF(UPPER(TRIM(sih.dt_inter)), ''), 'YYYYMMDD') AS dt_internacao
                , TO_DATE(NULLIF(UPPER(TRIM(sih.dt_saida)), ''), 'YYYYMMDD') AS dt_saida
                , CASE sih.morte::INTEGER 
                    WHEN 1 THEN 'S'
                    ELSE 'N'
                END AS fl_obito
                , sih.idade::INTEGER AS vl_idade_paciente
            FROM stg.stg_sih sih
        )
        SELECT
            mun_estab.sk_municipio AS sk_municipio_estabelecimento
            , mun_res.sk_municipio AS sk_municipio_residencia_paciente
            , estab.sk_estabelecimento
            , cid_prim.sk_cid AS sk_cid_diagnostico_primario
            , cid_sec.sk_cid AS sk_cid_diagnostico_secundario
            , sexo.sk_sexo
            , raca.sk_raca_cor
            , ocup.sk_ocupacao
            , proc_solic.sk_procedimento AS sk_procedimento_solicitado
            , proc_rea.sk_procedimento AS sk_procedimento_realizado
            , esp.sk_especialidade AS sk_especialidade_leito
            , stg.nu_competencia
            , stg.nu_aih
            , stg.ds_tipo_aih
            , stg.dt_nascimento_paciente
            , stg.qtd_diarias_internacao
            , stg.vl_total_internacao
            , stg.dt_internacao
            , stg.dt_saida
            , stg.fl_obito
            , stg.vl_idade_paciente
            , NOW() AS dt_carga
        FROM sih stg
        LEFT JOIN {schema}.{tb_name} fato
            ON (stg.nu_aih = fato.nu_aih
                AND stg.dt_nascimento_paciente = fato.dt_nascimento_paciente
                AND stg.nu_competencia = fato.nu_competencia
                AND stg.dt_internacao = fato.dt_internacao
                AND stg.dt_saida = fato.dt_saida)
        LEFT JOIN {schema}.d_municipio mun_estab
            ON (stg.cd_municipio_estabelecimento = mun_estab.cd_municipio_normalizado)
        LEFT JOIN {schema}.d_municipio mun_res
            ON (stg.cd_municipio_residencia_paciente = mun_res.cd_municipio_normalizado)
        LEFT JOIN {schema}.d_estabelecimento estab
            ON (stg.nu_cnpj_estabelecimento = estab.nu_cnpj_estabelecimento)
        LEFT JOIN {schema}.d_cid cid_prim
            ON (stg.cd_cid_diagnostico_primario = cid_prim.cd_cid)
        LEFT JOIN {schema}.d_cid cid_sec
            ON (stg.cd_cid_diagnostico_secundario = cid_sec.cd_cid)
        LEFT JOIN {schema}.d_sexo sexo
            ON (stg.cd_sexo = sexo.cd_sexo)
        LEFT JOIN {schema}.d_raca_cor raca
            ON (stg.cd_raca_cor = raca.cd_raca_cor)
        LEFT JOIN {schema}.d_ocupacao ocup
            ON (stg.cd_ocupacao = ocup.cd_ocupacao)
        LEFT JOIN {schema}.d_procedimento proc_solic
            ON (stg.cd_procedimento_solicitado = proc_solic.cd_procedimento)
        LEFT JOIN {schema}.d_procedimento proc_rea
            ON (stg.cd_procedimento_realizado = proc_rea.cd_procedimento)
        LEFT JOIN {schema}.d_especialidade esp
            ON (stg.cd_especialidade_leito = esp.cd_especialidade)
        WHERE fato.nu_aih IS NULL
    """

    return dwt.read_sql_query(query, con)


def treat_f_hospitalizacao(list_values, columns):
    dtypes = {
        'sk_municipio_estabelecimento': 'Int64',
        'sk_municipio_residencia_paciente': 'Int64',
        'sk_estabelecimento': 'Int64',
        'sk_cid_diagnostico_primario': 'Int64',
        'sk_cid_diagnostico_secundario': 'Int64',
        'sk_sexo': 'Int64',
        'sk_raca_cor': 'Int64',
        'sk_ocupacao': 'Int64',
        'sk_procedimento_solicitado': 'Int64',
        'sk_procedimento_realizado': 'Int64',
        'sk_especialidade_leito': 'Int64',
        'nu_competencia': 'string',
        'nu_aih': 'string',
        'ds_tipo_aih': 'string',
        'dt_nascimento_paciente': 'datetime64[ns]',
        'qtd_diarias_internacao': 'Int64',
        'vl_total_internacao': 'Float64',
        'dt_internacao': 'datetime64[ns]',
        'dt_saida': 'datetime64[ns]',
        'fl_obito': 'string',
        'vl_idade_paciente': 'Int64',
        'dt_carga': 'datetime64[ns]'
    }

    fillna = {
        'sk_municipio_estabelecimento': -2,
        'sk_municipio_residencia_paciente': -2,
        'sk_estabelecimento': -2,
        'sk_cid_diagnostico_primario': -2,
        'sk_cid_diagnostico_secundario': -2,
        'sk_sexo': -2,
        'sk_raca_cor': -2,
        'sk_ocupacao': -2,
        'sk_procedimento_solicitado': -2,
        'sk_procedimento_realizado': -2,
        'sk_especialidade_leito': -2,
        'nu_competencia': 'Não Informado',
        'nu_aih': 'Não Informado',
        'ds_tipo_aih': 'Não Informado',
        'dt_nascimento_paciente': dt.date(1900, 1, 1),
        'qtd_diarias_internacao': -2,
        'vl_total_internacao': -2,
        'dt_internacao': dt.date(1900, 1, 1),
        'dt_saida': dt.date(1900, 1, 1),
        'fl_obito': 'Não Informado',
        'vl_idade_paciente': -2,
        'dt_carga': dt.date(1900, 1, 1)
    }

    return pd.DataFrame(data=list_values)[columns].astype(dtypes).fillna(fillna)


def load_f_hospitalizacao(df, con, schema, tb_name, columns):
    dwt.load_with_csv(df, con, schema, tb_name, columns)


def run_f_hospitalizacao(con, schema, tb_name, chunck=10000):
    columns = dwt.get_table_columns(con, schema, tb_name)

    result = extract_f_hospitalizacao(con, schema, tb_name)

    while True:
        list_values = result.fetchmany(chunck)

        if not list_values:
            break
    
        treat_f_hospitalizacao(list_values, columns).pipe(load_f_hospitalizacao, con, schema, tb_name, columns)
