from rpy2.robjects import r
from abc import ABCMeta, abstractmethod


class Stg_Model(metaclass=ABCMeta):
    @classmethod
    def extract(cls, year_start, year_end, uf, info_sys):
        r(f"""
            rm(list = ls(all = TRUE))
            remotes::install_github("rfsaldanha/microdatasus")

            library(microdatasus)

            dados <- fetch_datasus(
                year_start = {year_start}, 
                year_end = {year_end}, 
                uf = "{uf}", 
                information_system = "{info_sys}")
        """)

    @abstractmethod
    def treat(self):
        raise NotImplementedError

    @classmethod
    def load(cls, df, con, schema, tb_name):
        df.to_sql(con=con, schema=schema, name=tb_name, if_exists='replace', index=False)

    @classmethod
    def run(cls, con, year_start, year_end, uf, info_sys, schema, tb_name):
        cls.extract(year_start, year_end, uf, info_sys)

        cls.treat().pipe(cls.load, con, schema, tb_name)
