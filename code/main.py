import pandas as pd
from datetime import datetime as dt
from pysus.online_data.SIM import download


def extract_sim(start_year,end_year,state):
    df = pd.concat([download(state,year) for year in range(start_year,end_year+1)])

    return df

def treat_sim(df: pd.DataFrame):
    columns = [col_name.lower() for col_name in df.columns]

    if 'numerodo' in columns:
        df['numerodo'] = df['numerodo'].astype(str)

    if 'codinst' in columns:
        df['codinst'] = df['codinst'].astype(str)

        df.loc[df['codinst'] == 'E', 'codinst'] = 'Estadual'
        df.loc[df['codinst'] == 'R', 'codinst'] = 'Regional'
        df.loc[df['codinst'] == 'M', 'codinst'] = 'Municipal'

    if 'numerodv' in columns:
        df['numerodv'] = df['numerodv'].astype(str)

    if 'origem' in columns:
        df['origem'] = df['origem'].astype(str)
    
    if 'tipobito' in columns:
        df['tipobito'] = df['tipobito'].astype(str)

        df.loc[df['tipobito'] == '0', 'tipobito'] = None
        df.loc[df['tipobito'] == '9', 'tipobito'] = None
        df.loc[df['tipobito'] == '1', 'tipobito'] = 'Fetal'
        df.loc[df['tipobito'] == '2', 'tipobito'] = 'Não Fetal'

    if 'dtobito' in columns:
        df['dtobito'] = df['dtobito'].astype(str).apply(lambda x:dt.strptime(x, '%d%m%Y').strftime('%d/%m/%Y'))
        df['dtobito'] = df['dtobito'].astype('datetime64[ns]')

    if 'horaobito' in columns:
        df['horaobito'] = df['horaobito'].astype(str)
    
    if 'numsus' in columns:
        df['numsus'] = df['numsus'].astype(str)

    if 'natural' in columns:
        df['natural'] = df['natural'].astype(str)
    'codmunnatu'
    'dtnasc'
    'idade'
    'minutos'
    'horas'
    'dias'
    'meses'
    'anos'
    'sexo'
    'racacor'
    'estciv'
    'esc'
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
