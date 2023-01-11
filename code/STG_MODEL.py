import os
import pandas as pd
from abc import ABCMeta


class Stg(metaclass=ABCMeta):
    @classmethod
    def extract(cls, folder):
        df = pd.concat([pd.read_csv(folder+f, sep=',', low_memory=False) for f in os.listdir(folder)])

        return df

    @classmethod
    def load(cls, df, conn, schema, name):
        df.to_sql(con=conn, schema=schema, name=name, if_exists='replace', chunksize=10000, method='multi')

    @classmethod
    def run(cls, folder, conn, schema, name):
        cls.extract(folder).pipe(cls.load, conn=conn, schema=schema, name=name)
