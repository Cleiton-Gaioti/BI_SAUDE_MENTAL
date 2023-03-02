import os
import pandas as pd
from ftplib import FTP
from dbfread import DBF


def extract_stg_uf():
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd("/dissemin/publicos/SIM/CID10/TABELAS")
    fname = "TABUF.DBF"
        
    try:
        ftp.retrbinary("RETR {}".format(fname), open(fname, "wb").write)
    except:
        raise Exception("Could not download {}".format(fname))

    dbf = DBF(fname, encoding="iso-8859-1")
    df = pd.DataFrame(list(dbf))

    os.unlink(fname)

    return df


def treat_stg_uf(df):
    df.columns = [col.lower() for col in df.columns]
    df = df.astype({"codigo": "Int64"})

    return df


def load_stg_uf(df, con, schema, tb_name, chunck):
    df.to_sql(name=tb_name, con=con, schema=schema, if_exists='replace', index=False, chunksize=chunck, method='multi')


def run_stg_uf(con, schema, tb_name, chunck=10000):
    extract_stg_uf().pipe(treat_stg_uf).pipe(load_stg_uf, con, schema, tb_name, chunck)
