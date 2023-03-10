import numpy as np
import pandas as pd
from DW_TOOLS import create_connection, cronometrar
from pysus.online_data.SIM import download, get_municipios


@cronometrar
def extract_sim(uf, start_year, end_year):
    df = pd.concat([download(uf, year) for year in range(start_year, end_year + 1)])

    return df


@cronometrar
def treat_sim(df):
    df = df.astype(str).replace({'': None, '<NA>': None})

    df_municipios = get_municipios().replace({'': None, '<NA>': None})

    df_naturalidade = pd.read_csv("dags/arquivos/tabNaturalidade.csv", sep=";", dtype=str).replace({'': None, '<NA>': None})

    df_ocupacao = pd.read_csv("dags/arquivos/tabOcupacao.csv", sep=";", dtype=str, names=['cod', 'nm_ocup']).replace({'': None, '<NA>': None})

    df_cbo = pd.read_csv("dags/arquivos/tabCBO.csv", sep=";", dtype=str, names=['cod', 'nm_cbo']).replace({'': None, '<NA>': None})

    df.columns = [col_name.lower() for col_name in df.columns]

    if 'codinst' in df.columns:
        df['codinst'] = df['codinst'].apply(
            lambda x:
            'Estadual'  if x == 'E' else
            'Regional'  if x == 'R' else
            'Municipal' if x == 'M' else
            None)

    if 'tipobito' in df.columns:
        df['tipobito'] = df['tipobito'].apply(
            lambda x:
            'Fetal'     if x == '1' else
            'Não Fetal' if x == '2' else
            None)

    if 'dtobito' in df.columns:
        df.loc[df['dtobito'] == '', 'dtobito'] = None
        df['dtobito'] = pd.to_datetime(df['dtobito'], format='%d%m%Y')

        df['dt_ano_obito'] = df['dtobito'].apply(lambda x: x.year)

    if 'natural' in df.columns:
        df['natural'] = df.merge(right=df_naturalidade, how='left', left_on='natural', right_on='cod')['nome']
    
    if 'dtnasc' in df.columns:
        df.loc[df['dtnasc'] == '', 'dtnasc'] = None
        df['dtnasc'] = pd.to_datetime(df['dtnasc'], format='%d%m%Y')

    if 'idade' in df.columns:
        df.loc[(df['idade'] == '000') | (df['idade'] == '999') | (df['idade'] == ''), 'idade'] = None
        df_none = df[df['idade'].isnull()]
        df_not_none = df[~df['idade'].isnull()]

        df_not_none = df_not_none.assign(
            idademinutos=lambda x: x['idade'].apply(lambda y: y[1:] if y[0] == '0' else None).astype('Int64'),
            idadehoras=lambda x: x['idade'].apply(lambda y: y[1:] if y[0] == '1' else None).astype('Int64'),
            idadedias=lambda x: x['idade'].apply(lambda y: y[1:] if y[0] == '2' else None).astype('Int64'),
            idademeses=lambda x: x['idade'].apply(lambda y: y[1:] if y[0] == '3' else None).astype('Int64'),
            idadeanos=lambda x: x['idade'].apply(lambda y: y[1:] if y[0] == '4' else ('1' + y[1:]) if y[0] == '5' else None).astype('Int64')
        )

        df = pd.concat([df_not_none, df_none])

        del df['idade']

    if 'sexo' in df.columns:
        df['sexo'] = df['sexo'].apply(
            lambda x:
            'Masculino' if x == '1' else
            'Feminino'  if x == '2' else
            None)

    if 'racacor' in df.columns:
        df = df.assign(racacor=lambda x: x['racacor'].apply(
            lambda y:
            'Branca'    if y == '1' else
            'Preta'     if y == '2' else
            'Amarela'   if y == '3' else
            'Parda'     if y == '4' else
            'Indígena'  if y == '5' else
            None))

    if 'estciv' in df.columns:
        df = df.assign(estciv=lambda x: x['estciv'].apply(
            lambda y:
            'Solteiro'                  if y == '1' else
            'Casado'                    if y == '2' else
            'Viúvo'                     if y == '3' else
            'Separado judicialmente'    if y == '4' else
            'União consensual'          if y == '5' else
            None))

    if 'esc' in df.columns:
        df = df.assign(esc=lambda x: x['esc'].apply(
            lambda y:
            'Nenhuma'           if y == '1' else
            '1 a 3 anos'        if y == '2' else
            '4 a 7 anos'        if y == '3' else
            '8 a 11 anos'       if y == '4' else
            '12 anos ou mais'   if y == '5' else
            '9 a 11 anos'       if y == '8' else
            None))

    if 'ocup' in df.columns:
        if not 'dtobito' in df.columns:
            raise "The variable DTOBITO is needed to preprocess the variable OCUP."
        else:
            df = df.merge(right=df_ocupacao, how='left', left_on='ocup', right_on='cod')
            del df['cod']

            df = df.merge(right=df_cbo, how='left', left_on='ocup', right_on='cod')

            df.loc[df['dt_ano_obito'] <= 2005, 'ocup'] = df.loc[df['dt_ano_obito'] <= 2005]['nm_ocup']
            df.loc[df['dt_ano_obito'] > 2005, 'ocup'] = df.loc[df['dt_ano_obito'] > 2005]['nm_cbo']
            df.loc[df['dt_ano_obito'].isnull(), 'ocup'] = None

            del df['nm_ocup'], df['nm_cbo'], df['cod']

    if 'codmunres' in df.columns:
        df_mun = df_municipios[['MUNCOD', 'MUNNOME', 'UFCOD']]
        df_mun = df_mun.rename(columns={'MUNCOD': 'codmunres', 'MUNNOME': 'nomemunres', 'UFCOD': 'codufres'})

        df = pd.merge(left=df, right=df_mun, how='left', on='codmunres')

        df['codmunres'] = df['codmunres'].apply(lambda x: x.strip())
        df['nomemunres'] = df['nomemunres'].apply(lambda x: x.strip())
        df['codufres'] = df['codufres'].apply(lambda x: x.strip())

    if 'lococor' in df.columns:
        df['lococor'] = df['lococor'].apply(
            lambda x:
            'Hospital'                          if x == '1' else
            'Outro estabelecimento de saúde'    if x == '2' else
            'Domicílio'                         if x == '3' else
            'Via pública'                       if x == '4' else
            'Outros'                            if x == '5' else
            None)

    if 'escmae' in df.columns:
        df = df.assign(escmae=lambda x: x['escmae'].apply(
            lambda y:
            'Nenhuma'           if y == '1' else
            '1 a 3 anos'        if y == '2' else
            '4 a 7 anos'        if y == '3' else
            '8 a 11 anos'       if y == '4' else
            '12 anos ou mais'   if y == '5' else
            '9 a 11 anos'       if y == '8' else
            None)
        )

    if 'ocupmae' in df.columns:
        if not 'dtobito' in df.columns:
            raise "The variable DTOBITO is needed to preprocess the variable OCUP."
        else:
            df = df.merge(right=df_ocupacao, how='left', left_on='ocupmae', right_on='cod')
            del df['cod']
            
            df = df.merge(right=df_cbo, how='left', left_on='ocupmae', right_on='cod')

            df.loc[df['dt_ano_obito'] <= 2005, 'ocupmae'] = df.loc[df['dt_ano_obito'] <= 2005]['nm_ocup']
            df.loc[df['dt_ano_obito'] > 2005, 'ocupmae'] = df.loc[df['dt_ano_obito'] > 2005]['nm_cbo']
            df.loc[df['dt_ano_obito'].isnull(), 'ocupmae'] = None

            del df['nm_ocup'], df['nm_cbo'], df['cod'], df['dt_ano_obito']

    if 'gravidez' in df.columns:
        df['gravidez'] = df['gravidez'].apply(
            lambda x:
            'Única'             if x == '1' else
            'Dupla'             if x == '2' else
            'Tríplice e mais'   if x == '3' else
            None)

    if 'gestacao' in df.columns:
        df['gestacao'] = df['gestacao'].apply(
            lambda x:
            '21 a 27 semanas'       if x == 'A' else
            'Menos de 22 semanas'   if x == '1' else
            '22 a 27 semanas'       if x == '2' else
            '28 a 31 semanas'       if x == '3' else
            '32 a 36 semanas'       if x == '4' else
            '37 a 41 semanas'       if x == '5' else
            '42 semanas e mais'     if x == '6' else
            '28 semanas e mais'     if x == '7' else
            '28 a 36 semanas'       if x == '8' else
            None)

    if 'parto' in df.columns:
        df['parto'] = df['parto'].apply(
            lambda x:
            'Vaginal'   if x == '1' else
            'Cesáreo'   if x == '2' else
            None)

    if 'obitoparto' in df.columns:
        df['obitoparto'] = df['obitoparto'].apply(
            lambda x:
            'Antezs'    if x == '1' else
            'Durante'   if x == '2' else
            'Depois'    if x == '3' else
            None)

    if 'peso' in df.columns:
        df['peso'] = df['peso'].astype('Int64')

    if 'obitograv' in df.columns:
        df['obitograv'] = df['obitograv'].apply(
            lambda x:
            'Sim'   if x == '1' else
            'Não'   if x == '2' else
            None)

    if 'obitopuerp' in df.columns:
        df['obitopuerp'] = df['obitopuerp'].apply(
            lambda x:
            'De 0 a 42 dias'        if x == '1' else
            'De 43 dias a 1 ano'    if x == '2' else
            'Não'                   if x == '3' else
            None)

    if 'assistmed' in df.columns:
        df['assistmed'] = df['assistmed'].apply(
            lambda x:
            'Sim'   if x == '1' else
            'Não'   if x == '2' else
            None)

    if 'exame' in df.columns:
        df['exame'] = df['exame'].apply(
            lambda x:
            'Sim'   if x == '1' else
            'Não'   if x == '2' else
            None)

    if 'cirurgia' in df.columns:
        df['cirurgia'] = df['cirurgia'].apply(
            lambda x:
            'Sim'   if x == '1' else
            'Não'   if x == '2' else
            None)

    if 'necropsia' in df.columns:
        df['necropsia'] = df['necropsia'].apply(
            lambda x:
            'Sim'   if x == '1' else
            'Não'   if x == '2' else
            None)

    if 'dtatestado' in df.columns:
        df.loc[df['dtatestado'] == '', 'dtatestado'] = None
        df['dtatestado'] = pd.to_datetime(df['dtatestado'], format='%d%m%Y')

    if 'circobito' in df.columns:
        df = df.assign(
            circobito=lambda x: x['circobito'].apply(
                lambda y:
                'Acidente'  if y == '1' else
                'Suicídio'  if y == '2' else
                'Homicídio' if y == '3' else
                'Outro'     if y == '4' else
                None
            )
        )

    if 'acidtrab' in df.columns:
        df['acidtrab'] = df['acidtrab'].apply(
            lambda x:
            'Sim'   if x == '1' else
            'Não'   if x == '2' else
            None)

    if 'fonte' in df.columns:
        df['fonte'] = df['fonte'].apply(
            lambda x:
            'Boletim de Ocorrência' if x == '1' else
            'Hospital'              if x == '2' else
            'Família'               if x == '3' else
            'Outro'                 if x == '4' else
            None)

    if 'tppos' in df.columns:
        df['tppos'] = df['tppos'].apply(
            lambda x:
            'Não investigado'   if x == 'N' else
            'Investigado'       if x == 'S' else
            None)

    if 'dtinvestig' in df.columns:
        df.loc[df['dtinvestig'] == '', 'dtinvestig'] = None
        df['dtinvestig'] = pd.to_datetime(df['dtinvestig'], format='%d%m%Y')

    if 'dtcadastro' in df.columns:
        df.loc[df['dtcadastro'] == '', 'dtcadastro'] = None
        df['dtcadastro'] = pd.to_datetime(df['dtcadastro'], format='%d%m%Y')

    if 'atestante' in df.columns:
        df = df.assign(
            atestante=lambda x: x['atestante'].apply(
                lambda y:
                'Sim'           if y == '1' else
                'Substituto'    if y == '2' else
                'IML'           if y == '3' else
                'SVO'           if y == '4' else
                'Outro'         if y == '5' else
                None
            )
        )

    if 'fonteinv' in df.columns:
        df = df.assign(
            fonteinv=lambda x: x['fonteinv'].apply(
                lambda y:
                'Comitê de Mortalidade Materna e/ou Infantil'   if y == '1' else
                'Visita familiar / Entrevista família'          if y == '2' else
                'Estabelecimento de saúde / Prontuário'         if y == '3' else
                'Relacionamento com outros bancos de dados'     if y == '4' else
                'SVO'                                           if y == '5' else
                'IML'                                           if y == '6' else
                'Outra fonte'                                   if y == '7' else
                'Múltiplas fontes'                              if y == '8' else
                None
            )
        )

    if 'dtrecebim' in df.columns:
        df.loc[df['dtrecebim'] == '', 'dtrecebim'] = None
        df['dtrecebim'] = pd.to_datetime(df['dtrecebim'], format='%d%m%Y')

    if 'dtrecoriga' in df.columns:
        df.loc[df['dtrecoriga'] == '', 'dtrecoriga'] = None
        df['dtrecoriga'] = pd.to_datetime(df['dtrecoriga'], format='%d%m%Y')

    df.replace({np.nan: None, 'nan': None}, inplace=True)

    return df


def load_sim(df, con, schema, tb_name):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='replace', index=False, chunksize=10000, method='multi')


def run_sim(uf, start_year, con, schema, tb_name, end_year=None):
    if not end_year:
        end_year = start_year

    extract_sim(uf, start_year, end_year).pipe(treat_sim).pipe(load_sim, con, schema, tb_name)


if __name__ == '__main__':
    con_out = create_connection('localhost', 'saude_mental', 'postgres', 'postgres', 5432)

    run_sim('ES', 2010, 2020, con_out, 'stg', 'sim_2010_2020')
