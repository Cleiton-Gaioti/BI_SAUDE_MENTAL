import os
import pandas as pd


def extract(sigla_estado=''):
    folder = './datasources/arquivos_sim/'
    init = f'ETLSIM.DORES_{sigla_estado}'

    df = pd.concat([pd.read_csv(folder + f, sep=',', low_memory=False) for f in os.listdir(folder) if f.startswith(init)])

    return df


df = extract('ES')

print(df.shape)