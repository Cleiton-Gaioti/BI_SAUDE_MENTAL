import pandas as pd
import datetime as dt
import DW_TOOLS as dwt


@dwt.cronometrar
def extract_d_estabelecimento(con, schema, tb_name):
    query = f"""
        WITH stage AS (
            SELECT DISTINCT
                NULLIF(TRIM(cgc_hosp), '') AS nu_cnpj_estabelecimento
                , CASE
                    WHEN NULLIF(UPPER(TRIM(natureza)), '')::INTEGER IN (10,30,31,40,41,50)
                        THEN 'PÚBLICO'
                    WHEN NULLIF(UPPER(TRIM(natureza)), '')::INTEGER IN (20,22,60,61,63,80)
                        THEN 'PRIVADO'
                    WHEN NULLIF(UPPER(TRIM(natureza)), '')::INTEGER IN (70,90,91,92,93,94)
                        THEN 'UNIVERSITÁRIO'
                    ELSE 'Não Informado'
                END AS ds_regime
                , CASE NULLIF(UPPER(TRIM(nat_jur)), '')::INTEGER
                    WHEN 1015 THEN 'ÓRGÃO PÚBLICO DO PODER EXECUTIVO FEDERAL'
                    WHEN 1023 THEN 'ÓRGÃO PÚBLICO DO PODER EXEC ESTADUAL OU DISTR FED'
                    WHEN 1031 THEN 'ÓRGÃO PÚBLICO DO PODER EXECUTIVO MUNICIPAL'
                    WHEN 1040 THEN 'ÓRGÃO PÚBLICO DO PODER LEGISLATIVO FEDERAL'
                    WHEN 1058 THEN 'ÓRGÃO PÚBLICO DO PODER LEGISL ESTADUAL OU DIST FED'
                    WHEN 1066 THEN 'ÓRGÃO PÚBLICO DO PODER LEGISLATIVO MUNICIPAL'
                    WHEN 1074 THEN 'ÓRGÃO PÚBLICO DO PODER JUDICIÁRIO FEDERAL'
                    WHEN 1082 THEN 'ÓRGÃO PÚBLICO DO PODER JUDICIÁRIO ESTADUAL'
                    WHEN 1104 THEN 'AUTARQUIA FEDERAL'
                    WHEN 1112 THEN 'AUTARQUIA ESTADUAL OU DO DISTRITO FEDERAL'
                    WHEN 1120 THEN 'AUTARQUIA MUNICIPAL'
                    WHEN 1139 THEN 'FUNDAÇÃO FEDERAL'
                    WHEN 1147 THEN 'FUNDAÇÃO ESTADUAL OU DO DISTRITO FEDERAL'
                    WHEN 1155 THEN 'FUNDAÇÃO MUNICIPAL'
                    WHEN 1163 THEN 'ÓRGÃO PÚBLICO AUTÔNOMO FEDERAL'
                    WHEN 1171 THEN 'ÓRGÃO PÚBLICO AUTÔNOMO ESTADUAL OU DISTR FEDERAL'
                    WHEN 1180 THEN 'ÓRGÃO PÚBLICO AUTÔNOMO ESTADUAL OU DISTR FEDERAL'
                    WHEN 1198 THEN 'COMISSÃO POLINACIONAL'
                    WHEN 1201 THEN 'FUNDO PÚBLICO'
                    WHEN 1210 THEN 'ASSOCIAÇÃO PÚBLICA'
                    WHEN 2011 THEN 'EMPRESA PÚBLICA'
                    WHEN 2038 THEN 'SOCIEDADE DE ECONOMIA MISTA'
                    WHEN 2046 THEN 'SOCIEDADE ANÔNIMA ABERTA'
                    WHEN 2054 THEN 'SOCIEDADE ANÔNIMA FECHADA'
                    WHEN 2062 THEN 'SOCIEDADE EMPRESÁRIA LIMITADA'
                    WHEN 2070 THEN 'SOCIEDADE EMPRESÁRIA EM NOME COLETIVO'
                    WHEN 2089 THEN 'SOCIEDADE EMPRESÁRIA EM COMANDITA SIMPLES'
                    WHEN 2097 THEN 'SOCIEDADE EMPRESÁRIA EM COMANDITA POR AÇÕES'
                    WHEN 2127 THEN 'SOCIEDADE EM CONTA DE PARTICIPAÇÃO'
                    WHEN 2135 THEN 'EMPRESÁRIO (INDIVIDUAL)'
                    WHEN 2143 THEN 'COOPERATIVA'
                    WHEN 2151 THEN 'CONSÓRCIO DE SOCIEDADES'
                    WHEN 2160 THEN 'GRUPO DE SOCIEDADES'
                    WHEN 2178 THEN 'ESTABELECIMENTO NO BRASIL DE SOCIEDADE ESTRANGEIRA'
                    WHEN 2194 THEN 'ESTAB NO BRASIL EMPR BINACIONAL ARGENTINA-BRASIL'
                    WHEN 2216 THEN 'EMPRESA DOMICILIADA NO EXTERIOR'
                    WHEN 2224 THEN 'CLUBE/FUNDO DE INVESTIMENTO'
                    WHEN 2232 THEN 'SOCIEDADE SIMPLES PURA'
                    WHEN 2240 THEN 'SOCIEDADE SIMPLES LIMITADA'
                    WHEN 2259 THEN 'SOCIEDADE SIMPLES EM NOME COLETIVO'
                    WHEN 2267 THEN 'SOCIEDADE SIMPLES EM COMANDITA SIMPLES'
                    WHEN 2275 THEN 'EMPRESA BINACIONAL'
                    WHEN 2283 THEN 'CONSÓRCIO DE EMPREGADORES'
                    WHEN 2291 THEN 'CONSÓRCIO SIMPLES'
                    WHEN 2305 THEN 'EMPR INDIVID RESPONSAB LIMITADA (NATUR EMPRESÁRIA)'
                    WHEN 2313 THEN 'EMPR INDIVID RESPONSAB LIMITADA (NATUREZA SIMPLES)'
                    WHEN 3034 THEN 'SERVIÇO NOTARIAL E REGISTRAL (CARTÓRIO)'
                    WHEN 3069 THEN 'FUNDAÇÃO PRIVADA'
                    WHEN 3077 THEN 'SERVIÇO SOCIAL AUTÔNOMO'
                    WHEN 3085 THEN 'CONDOMÍNIO EDILÍCIO'
                    WHEN 3107 THEN 'COMISSÃO DE CONCILIAÇÃO PRÉVIA'
                    WHEN 3115 THEN 'ENTIDADE DE MEDIAÇÃO E ARBITRAGEM'
                    WHEN 3123 THEN 'PARTIDO POLÍTICO'
                    WHEN 3131 THEN 'ENTIDADE SINDICAL'
                    WHEN 3204 THEN 'ESTAB NO BRASIL DE FUNDAÇÃO OU ASSOCIAÇÃO ESTRANG'
                    WHEN 3212 THEN 'FUNDAÇÃO OU ASSOCIAÇÃO DOMICILIADA NO EXTERIOR'
                    WHEN 3220 THEN 'ORGANIZAÇÃO RELIGIOSA'
                    WHEN 3239 THEN 'COMUNIDADE INDÍGENA'
                    WHEN 3247 THEN 'FUNDO PRIVADO'
                    WHEN 3999 THEN 'ASSOCIAÇÃO PRIVADA'
                    WHEN 4014 THEN 'EMPRESA INDIVIDUAL IMOBILIÁRIA'
                    WHEN 4022 THEN 'SEGURADO ESPECIAL'
                    WHEN 4081 THEN 'CONTRIBUINTE INDIVIDUAL'
                    WHEN 4090 THEN 'CANDIDATO A CARGO POLÍTICO ELETIVO'
                    WHEN 4111 THEN 'LEILOEIRO'
                    WHEN 5010 THEN 'ORGANIZAÇÃO INTERNACIONAL'
                    WHEN 5029 THEN 'REPRESENTAÇÃO DIPLOMÁTICA ESTRANGEIRA'
                    WHEN 5037 THEN 'OUTRAS INSTITUIÇÕES EXTRATERRITORIAIS'
                    ELSE 'Não Informado'
                END AS ds_natureza_juridica
                , CASE
                    WHEN NULLIF(UPPER(TRIM(gestao)), '') = '0' THEN 'ESTADUAL'
                    WHEN NULLIF(UPPER(TRIM(gestao)), '') IN ('1', 'M') THEN 'MUNICIPAL PLENA ASSIST'
                    WHEN NULLIF(UPPER(TRIM(gestao)), '') IN ('2', 'E') THEN 'ESTADUAL PLENA'
                    ELSE 'Não Informado' 
                END AS ds_gestao
            FROM STG.stg_sih)
        SELECT 
            st.nu_cnpj_estabelecimento
            , MAX(st.ds_regime) AS ds_regime
            , MAX(st.ds_natureza_juridica) AS ds_natureza_juridica
            , MAX(st.ds_gestao) AS ds_gestao
            , NOW() AS dt_carga
        FROM stage st
        LEFT JOIN {schema}.{tb_name} dim
            ON (st.nu_cnpj_estabelecimento = dim.nu_cnpj_estabelecimento)
        WHERE dim.nu_cnpj_estabelecimento IS NULL
        GROUP BY st.nu_cnpj_estabelecimento
    """

    return dwt.read_table_from_sql(query, con)


def treat_d_estabelecimento(df, max_sk):
    df.insert(0, "sk_estabelecimento", range(max(1, max_sk), len(df) + max(1, max_sk)))

    if not max_sk:
        dt_default = dt.datetime(1900, 1, 1)

        df_aux = pd.DataFrame(data=[
            [-1, 'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado', dt_default],
            [-2, 'Não Aplicável', 'Não Aplicável', 'Não Aplicável', 'Não Aplicável', dt_default],
            [-3, 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido', dt_default]
        ], columns=df.columns)

        df = pd.concat([df_aux, df])

    return df


def load_d_estabelecimento(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='append', index=False, chunksize=chunck, method='multi')


def run_d_estabelecimento(con, schema, tb_name, chunck=10000):
    tbl_extract = extract_d_estabelecimento(con, schema, tb_name)

    if not tbl_extract.empty:
        dim_size = dwt.get_dimension_size(con, schema, tb_name)
        treat_d_estabelecimento(tbl_extract, dim_size).pipe(load_d_estabelecimento, con, schema, tb_name, chunck)
