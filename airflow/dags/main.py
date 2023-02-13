import pandas as pd
from datetime import datetime as dt
from CONNECTION import create_connection
from pysus.online_data.SIM import download, get_municipios, get_ocupations


def extract_sim(uf, start_year, end_year):
    df = pd.concat([download(uf, year) for year in range(start_year, end_year + 1)])

    return df


def treat_sim(df):
    df = df.astype(str)
    df_municipios = get_municipios().astype(str).rename({'MUNCOD': 'CODMUNRES'})

    columns = [col_name.lower() for col_name in df.columns]

    if 'codinst' in columns:
        df.loc[df['codinst'] == 'E', 'codinst'] = 'Estadual'
        df.loc[df['codinst'] == 'R', 'codinst'] = 'Regional'
        df.loc[df['codinst'] == 'M', 'codinst'] = 'Municipal'

    if 'tipobito' in columns:
        df.loc[(df['tipobito'] == '0') | (df['tipobito'] == '9'), 'tipobito'] = None
        df.loc[df['tipobito'] == '1', 'tipobito'] = 'Fetal'
        df.loc[df['tipobito'] == '2', 'tipobito'] = 'Não Fetal'

    if 'dtobito' in columns:
        df.loc[df['dtobito'] == '', 'dtobito'] = None
        df['dtobito'] = df['dtobito'].apply(lambda x: dt.strptime(x, '%d%m%Y').strftime('%d/%m/%Y'))
        df['dtobito'] = df['dtobito'].astype('datetime64[ns]')

    if 'natural' in columns:
        pass # TODO: estudar tratamentos a serem feitos

    if 'dtnasc' in columns:
        df.loc[df['dtnasc'] == '', 'dtnasc'] = None
        df['dtnasc'] = df['dtnasc'].apply(lambda x: dt.strptime(x, '%d%m%Y').strftime('%d/%m/%Y'))

    if 'idade' in columns:
        df.loc[(df['idade'] == '000') | (df['idade'] == '999'), 'idade'] = None

        df = df.assign(
            idademinutos=lambda x: x['idade'].apply(lambda y: y[1:] if y[0] == '0' else None).astype('Int64'),
            idadehoras=lambda x: x['idade'].apply(lambda y: y[1:] if y[0] == '1' else None).astype('Int64'),
            idadedias=lambda x: x['idade'].apply(lambda y: y[1:] if y[0] == '2' else None).astype('Int64'),
            idademeses=lambda x: x['idade'].apply(lambda y: y[1:] if y[0] == '3' else None).astype('Int64'),
            idadeanos=lambda x: x['idade'].apply(
                lambda y: y[1:] if y[0] == '4' else ('1' + y[1:]) if y[0] == '5' else None).astype('Int64'),
            idade=None
        )

    if 'sexo' in columns:
        df.loc[(df['sexo'] == '0') | (df['sexo'] == '9'), 'sexo'] = None
        df.loc[df['sexo'] == '1', 'sexo'] = 'Masculino'
        df.loc[df['sexo'] == '2', 'sexo'] = 'Feminino'

    if 'racacor' in columns:
        df = df.assign(racacor=lambda x: x['racacor'].apply(
            lambda y:
            'Branca'    if y == '1' else
            'Preta'     if y == '2' else
            'Amarela'   if y == '3' else
            'Parda'     if y == '4' else
            'Indígena'  if y == '5' else
            None))

    if 'estciv' in columns:
        df = df.assign(estciv=lambda x: x['estciv'].apply(
            lambda y:
            'Solteiro'                  if y == '1' else
            'Casado'                    if y == '2' else
            'Viúvo'                     if y == '3' else
            'Separado judicialmente'    if y == '4' else
            'União consensual'          if y == '5' else
            None))

    if 'esc' in columns:
        df = df.assign(esc=lambda x: x['esc'].apply(
            lambda y:
            'Nenhuma'           if y == '1' else
            '1 a 3 anos'        if y == '2' else
            '4 a 7 anos'        if y == '3' else
            '8 a 11 anos'       if y == '4' else
            '12 anos ou mais'   if y == '5' else
            '9 a 11 anos'       if y == '8' else
            None))

    if 'ocup' in columns:
        if not 'dtobito' in columns:
            raise ValueError('The variable DTOBITO is needed to preprocess the variable OCUP.')

        pass # TODO: estudar tratamentos a serem feitos

    if 'codmunres' in columns:
        df_mun = df_municipios[['MUNCOD', 'MUNNOME', 'UFCOD']]
        df_mun = df_mun.rename(columns={'MUNCOD': 'codmunres', 'MUNNOME': 'nomemunres', 'UFCOD': 'codufres'})
        
        df = pd.merge(left=df, right=df_mun, how='left', on='codmunres')

    if 'lococor' in columns:
        df.loc[df['lococor'] == '1', 'lococor'] = 'Hospital'
        df.loc[df['lococor'] == '2', 'lococor'] = 'Outro estabelecimento de saúde'
        df.loc[df['lococor'] == '3', 'lococor'] = 'Domicílio'
        df.loc[df['lococor'] == '4', 'lococor'] = 'Via pública'
        df.loc[df['lococor'] == '5', 'lococor'] = 'Outros'
        df.loc[df['lococor'] == '6', 'lococor'] = None

    if 'escmae' in columns:
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

    if 'ocupmae' in columns:
        if not 'dtobito' in columns:
            raise ValueError('The variable DTOBITO is needed to preprocess the variable OCUPMAE.')
        pass # TODO: estudar tratamentos a serem feitos

    if 'gravidez' in columns:
        df.loc[df['gravidez'] == '1', 'gravidez'] = 'Única'
        df.loc[df['gravidez'] == '2', 'gravidez'] = 'Dupla'
        df.loc[df['gravidez'] == '3', 'gravidez'] = 'Tríplice e mais'
        df.loc[df['gravidez'] == '9', 'gravidez'] = None
    
    if 'gestacao' in columns:
        df.loc[(df['gestacao'] == '0') | (df['gestacao'] == '9'), 'gestacao'] = None
        df.loc[df['gestacao'] == 'A', 'gestacao'] = '21 a 27 semanas'
        df.loc[df['gestacao'] == '1', 'gestacao'] = 'Menos de 22 semanas'
        df.loc[df['gestacao'] == '2', 'gestacao'] = '22 a 27 semanas'
        df.loc[df['gestacao'] == '3', 'gestacao'] = '28 a 31 semanas'
        df.loc[df['gestacao'] == '4', 'gestacao'] = '32 a 36 semanas'
        df.loc[df['gestacao'] == '5', 'gestacao'] = '37 a 41 semanas'
        df.loc[df['gestacao'] == '6', 'gestacao'] = '42 semanas e mais'
        df.loc[df['gestacao'] == '7', 'gestacao'] = '28 semanas e mais'
        df.loc[df['gestacao'] == '8', 'gestacao'] = '28 a 36 semanas'

    if 'parto' in columns:
        df.loc[df['parto'] == '1', 'parto'] = 'Vaginal'
        df.loc[df['parto'] == '2', 'parto'] = 'Cesáreo'
        df.loc[(df['parto'] != '1') & df['parto'] != '2', 'parto'] = None

    if 'obitoparto' in columns:
        df.loc[df['obitoparto'] == '1', 'obitoparto'] = 'Antezs'
        df.loc[df['obitoparto'] == '2', 'obitoparto'] = 'Durante'
        df.loc[df['obitoparto'] == '3', 'obitoparto'] = 'Depois'
        df.loc[(df['obitoparto'] != '1') & (df['obitoparto'] != '2') & (df['obitoparto'] != '3'), 'obitoparto'] = None
    
    
    if 'peso' in columns:
        df.loc[df['peso'] == '', 'peso'] == None
        df['peso'] = df['peso'].astype('Int64')

    if 'obitograv' in columns:
        df.loc[df['obitograv'] == '1', 'obitograv'] = 'Sim'
        df.loc[df['obitograv'] == '2', 'obitograv'] = 'Não'
        df.loc[(df['obitograv'] != '1') & (df['obitograv'] != '2'), 'obitograv'] = None

    if 'obitopuerp' in columns:
        df.loc[df['obitopuerp'] == '1', 'obitopuerp'] = 'De 0 a 42 dias'
        df.loc[df['obitopuerp'] == '2', 'obitopuerp'] = 'De 43 dias a 1 ano'
        df.loc[df['obitopuerp'] == '3', 'obitopuerp'] = 'Não'
        df.loc[(df['obitopuerp'] != '1') & (df['obitopuerp'] != '2') & (df['obitopuerp'] != '3'), 'obitopuerp'] = None

    if 'assistmed' in columns:
        df.loc[df['assistmed'] == '1', 'assistmed'] = 'Sim'
        df.loc[df['assistmed'] == '2', 'assistmed'] = 'Não'
        df.loc[(df['assistmed'] != '1') & (df['assistmed'] != '2'), 'assistmed'] = 'Sim'
    
    if 'exame' in columns:
        df.loc[df['exame'] == '1', 'exame'] = 'Sim'
        df.loc[df['exame'] == '2', 'exame'] = 'Não'
        df.loc[(df['exame'] != '1') & (df['exame'] != '2'), 'exame'] = None

    if 'cirurgia' in columns:
        df.loc[df['cirurgia'] == '1', 'cirurgia'] = 'Sim'
        df.loc[df['cirurgia'] == '2', 'cirurgia'] = 'Não'
        df.loc[(df['cirurgia'] != '1') & (df['cirurgia'] != '2'), 'cirurgia'] = None

    if 'necropsia' in columns:
        df.loc[df['necropsia'] == '1', 'necropsia'] = 'Sim'
        df.loc[df['necropsia'] == '2', 'necropsia'] = 'Não'
        df.loc[df['necropsia'] == '1', 'necropsia'] = None

    if 'dtatestado' in columns:
        df.loc[df['dtatestado'] == '', 'dtatestado'] = None
        df['dtatestado'] = df['dtatestado'].apply(lambda x: dt.strptime(x, '%d%m%Y').strftime('%d/%m/%Y'))
        df['dtatestado'] = df['dtatestado'].astype('datetime64[ns]')

    if 'circobito' in columns:
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

    if 'acidtrab' in columns:
        df.loc[df['acidtrab'] == '1', 'acidtrab'] = 'Sim'
        df.loc[df['acidtrab'] == '2', 'acidtrab'] = 'Não'
        df.loc[(df['acidtrab'] != '1') & (df['acidtrab'] != '2'), 'acidtrab'] = None

    if 'fonte' in columns:
        df.loc[df['fonte'] == '1', 'fonte'] = 'Boletim de Ocorrência'
        df.loc[df['fonte'] == '2', 'fonte'] = 'Hospital'
        df.loc[df['fonte'] == '3', 'fonte'] = 'Família'
        df.loc[df['fonte'] == '4', 'fonte'] = 'Outro'
        df.loc[(df['fonte'] != '1') & (df['fonte'] != '2') & (df['fonte'] != '3') & (df['fonte'] != '4'), 'fonte'] = None

    if 'tppos' in columns:
        df.loc[df['tppos'] == 'N', 'tppos'] = 'Não investigado'
        df.loc[df['tppos'] == 'S', 'tppos'] = 'Investigado'
        df.loc[(df['tppos'] != 'N') & (df['tppos'] != 'S'), 'tppos'] = None

    if 'dtinvestig' in columns:
        df.loc[df['dtinvestig'] == '', 'dtinvestig'] = None
        df['dtinvestig'] = df['dtinvestig'].apply(lambda x: dt.strptime(x, '%d%m%Y').strftime('%d/%m/%Y'))
        df['dtinvestig'] = df['dtinvestig'].astype('datetime64[ns]')

    if 'dtcadastro' in columns:
        df.loc[df['dtcadastro'] == '', 'dtcadastro'] = None
        df['dtcadastro'] = df['dtcadastro'].apply(lambda x: dt.strptime(x, '%d%m%Y').strftime('%d/%m/%Y'))
        df['dtcadastro'] = df['dtcadastro'].astype('datetime64[ns]')
    
    if 'atestante' in columns:
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

    if 'fonteinv' in columns:
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

    if 'dtrecebim' in columns:
        df.loc[df['dtrecebim'] == '', 'dtrecebim'] = None
        df['dtrecebim'] = df['dtrecebim'].apply(lambda x: dt.strptime(x, '%d%m%Y').strftime('%d/%m/%Y'))
        df['dtrecebim'] = df['dtrecebim'].astype('datetime64[ns]')  

    if 'dtrecoriga' in columns:
        df.loc[df['dtrecoriga'] == '', 'dtrecoriga'] = None
        df['dtrecoriga'] = df['dtrecoriga'].apply(lambda x: dt.strptime(x, '%d%m%Y').strftime('%d/%m/%Y'))
        df['dtrecoriga'] = df['dtrecoriga'].astype('datetime64[ns]')
    
    return df


def load_sim(df, con, schema, tb_name):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='replace', index=False, chunksize=10000, method='multi')


def run_sim(uf, start_year, end_year, con, schema, tb_name):
    extract_sim(uf, start_year, end_year).pipe(treat_sim).pipe(load_sim, con, schema, tb_name)


if __name__ == '__main__':
    con_out = create_connection('localhost', 'saude_mental', 'postgres', 'postgres', 5432)

    run_sim('ES', 2010, 2020, con_out, 'stg', 'sim_2010_2020')
