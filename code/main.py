import pandas as pd
from datetime import datetime as dt
from pysus.online_data.SIM import download, get_municipios


def extract_sim(start_year, end_year, state):
    df = pd.concat([download(state, year) for year in range(start_year, end_year + 1)])

    return df


def treat_sim(df: pd.DataFrame):
    df = df.astype(str)
    df_municipios = get_municipios()

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
        df['dtobito'] = df['dtobito'].apply(lambda x: dt.strptime(x, '%d%m%Y').strftime('%d/%m/%Y'))
        df['dtobito'] = df['dtobito'].astype('datetime64[ns]')

    if 'dtnasc' in columns:
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
            'Branca' if y == '1' else
            'Preta' if y == '2' else
            'Amarela' if y == '3' else
            'Parda' if y == '4' else
            'Indígena' if y == '5' else
            None))

    if 'estciv' in columns:
        df = df.assign(estciv=lambda x: x['estciv'].apply(
            lambda y:
            'Solteiro' if y == '1' else
            'Casado' if y == '2' else
            'Viúvo' if y == '3' else
            'Separado judicialmente' if y == '4' else
            'União consensual' if y == '5' else
            None))

    if 'esc' in columns:
        df = df.assign(esc=lambda x: x['esc'].apply(
            lambda y:
            'Nenhuma' if y == '1' else
            '1 a 3 anos' if y == '2' else
            '4 a 7 anos' if y == '3' else
            '8 a 11 anos' if y == '4' else
            '12 anos ou mais' if y == '5' else
            '9 a 11 anos' if y == '8' else
            None))

    'esc2010'
    'seriescfal'
    'ocup'
    'codmunres'
    'lococor'
    'codestab'
    'estabdescr'
    'codmunocor'
    'idademae'
    'escmae'
    'escmae2010'
    'seriescmae'
    'ocupmae'
    'qtdfilvivo'
    'qtdfilmort'
    'gravidez'
    'semagestac'
    'gestacao'
    'parto'
    'obitoparto'
    'peso'
    'tpmorteoco'
    'obitograv'
    'obitopuerp'
    'assistmed'
    'exame'
    'cirurgia'
    'necropsia'
    'linhaa'
    'linhab'
    'linhac'
    'linhad'
    'linhaii'
    'causabas'
    'cb_pre'
    'crm'
    'comunsvoim'
    'dtatestado'
    'circobito'
    'acidtrab'
    'fonte'
    'numerolote'
    'tppos'
    'dtinvestig'
    'causabas_o'
    'dtcadastro'
    'atestante'
    'stcodifica'
    'codificado'
    'versaosist'
    'versaoscb'
    'fonteinv'
    'dtrecebim'
    'atestado'
    'dtrecoriga'
    'causamat'
    'escmaeagr1'
    'escfalagr1'
    'stdoepidem'
    'stdonova'
    'difdata'
    'nudiasobco'
    'nudiasobin'
    'dtcadinv'
    'tpobitocor'
    'dtconinv'
    'fontes'
    'tpresginfo'
    'tpnivelinv'
    'nudiasinf'
    'dtcadinf'
    'morteparto'
    'dtconcaso'
    'fontesinf'
    'altcausa'
