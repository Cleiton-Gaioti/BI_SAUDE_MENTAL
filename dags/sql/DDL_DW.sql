CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.d_municipio (
    sk_municipio                INT PRIMARY KEY,
    cd_municipio                INT,
    cd_municipio_normalizado    INT,
    no_municipio                TEXT,
    cd_microrregiao             INT,
    no_microrregiao             TEXT,
    cd_mesorregiao              INT,
    no_mesorregiao              TEXT,
    cd_regiao_imediata          INT,
    no_regiao_imediata          TEXT,
    cd_regiao_intermediaria     INT,
    no_regiao_intermediaria     TEXT,
    cd_uf                       INT,
    ds_sigla_uf                 TEXT,
    no_uf                       TEXT,
    cd_regiao                   INT,
    ds_sigla_regiao             TEXT,
    no_regiao                   TEXT,
    dt_carga                    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw.d_naturalidade (
    sk_naturalidade INT PRIMARY KEY,
    cd_naturalidade INT,
    ds_naturalidade TEXT,
    ds_nivel        TEXT,
    dt_carga        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw.d_ocupacao (
    sk_ocupacao INT PRIMARY KEY, 
    cd_ocupacao INT, 
    ds_ocupacao TEXT,
    dt_carga    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw.d_raca_cor (
    sk_raca_cor INT PRIMARY KEY,
    cd_raca_cor INT,
    ds_raca_cor TEXT,
    dt_carga    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw.d_cid (
    sk_cid              INT PRIMARY KEY,
    cd_cid              TEXT,
    ds_capitulo         TEXT,
    ds_categoria        TEXT,
    ds_subcategoria     TEXT,
    fl_restricao_sexo   TEXT,
    dt_carga            TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw.d_sexo (
    sk_sexo     INT PRIMARY KEY,
    cd_sexo     INT,
    ds_sexo     TEXT,
    dt_carga    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw.d_estabelecimento (
    sk_estabelecimento      INT PRIMARY KEY,
    nu_cnpj_estabelecimento TEXT,
    ds_regime               TEXT,
    ds_natureza_juridica    TEXT,
    ds_gestao               TEXT,
    dt_carga                TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw.d_especialidade (
    sk_especialidade    INT PRIMARY KEY,
    cd_especialidade    INT,
    ds_especialidade    TEXT,
    dt_carga            TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw.d_procedimento (
    sk_procedimento INT PRIMARY KEY,
    cd_procedimento INT,
    ds_procedimento TEXT,
    dt_carga        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw.f_obito (
    sk_municipio_naturalidade_falecido  INT REFERENCES dw.d_municipio       (sk_municipio),
    sk_municipio_ocorrencia_obito       INT REFERENCES dw.d_municipio       (sk_municipio),
    sk_municipio_residencia_falecido    INT REFERENCES dw.d_municipio       (sk_municipio),
    sk_ocupacao_falecido                INT REFERENCES dw.d_ocupacao        (sk_ocupacao),
    sk_naturalidade                     INT REFERENCES dw.d_naturalidade    (sk_naturalidade),
    sk_raca_cor                         INT REFERENCES dw.d_raca_cor        (sk_raca_cor),
    sk_cid_causa_obito                  INT REFERENCES dw.d_cid             (sk_cid),
    sk_sexo                             INT REFERENCES dw.d_sexo            (sk_sexo),
    cd_registro                         INT,
    nu_cid                              TEXT,
    ds_assistencia_medica               TEXT,
    ds_atestante                        TEXT,
    ds_circunstancia_obito              TEXT,
    nu_crm_medico_atestante             TEXT,
    dt_cadastro                         DATE,
    dt_atestado                         DATE,
    dt_obito                            DATE,
    dt_nascimento_falecido              DATE,
    vl_idade_falecido                   INT,
    dt_carga                            TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dw.f_hospitalizacao (
    sk_municipio_estabelecimento        INT REFERENCES dw.d_municipio       (sk_municipio),
    sk_municipio_residencia_paciente    INT REFERENCES dw.d_municipio       (sk_municipio),
    sk_estabelecimento                  INT REFERENCES dw.d_estabelecimento (sk_estabelecimento),
    sk_cid_diagnostico_primario         INT REFERENCES dw.d_cid             (sk_cid),
    sk_cid_diagnostico_secundario       INT REFERENCES dw.d_cid             (sk_cid),
    sk_sexo                             INT REFERENCES dw.d_sexo            (sk_sexo),
    sk_raca_cor                         INT REFERENCES dw.d_raca_cor        (sk_raca_cor),
    sk_ocupacao                         INT REFERENCES dw.d_ocupacao        (sk_ocupacao),
    sk_procedimento_solicitado          INT REFERENCES dw.d_procedimento    (sk_procedimento),
    sk_procedimento_realizado           INT REFERENCES dw.d_procedimento    (sk_procedimento),
    sk_especialidade_leito              INT REFERENCES dw.d_especialidade   (sk_especialidade),
    nu_competencia                      TEXT,
    nu_aih                              TEXT,
    ds_tipo_aih                         TEXT,
    dt_nascimento_paciente              TIMESTAMP,
    qtd_diarias_internacao              INT,
    vl_total_internacao                 FLOAT,
    dt_internacao                       TIMESTAMP,
    dt_saida                            TIMESTAMP,
    fl_obito                            TEXT,
    vl_idade_paciente                   INT,
    dt_carga                            TIMESTAMP
);
