from rpy2.robjects import r
from abc import ABCMeta, abstractmethod


class Stg_Model(metaclass=ABCMeta):
    @classmethod
    def extract(cls, year_start, year_end, uf, info_sys):
        r(f"""
            options(repos=c(CRAN="https://cloud.r-project.org/bin/windows/contrib/4.2/remotes_2.4.2.zip"))
        
            install.packages('remotes')
            remotes::install_github('rfsaldanha/microdatasus')
        
            library(DBI)
            library(microdatasus)
        
            dados <- fetch_datasus(
                year_start={year_start}, 
                year_end={year_end}, 
                uf='{uf}', 
                information_system='{info_sys}')

            dados <- process_sim(data)
        """)

    @abstractmethod
    def treat(self):
        raise NotImplementedError

    @classmethod
    def load(cls, dbname, host, port, user, password, schema, table_name):
        r(f"""
            install.packages(c('devtools', 'RPostgres'))
            
            tryCatch({{
                print('Connecting to Database…')
        
                con <- dbConnect(
                    RPostgres::Postgres(),
                    dbname='{dbname}',
                    host='{host}',
                    port='{port}',
                    user='{user}',
                    password='{password}')

                print('Database Connected!')
            }},
            error=function(cond) {{
                print('Unable to connect to Database.')
            }})

            dbWriteTable(conn=con, name=Id(schema='{schema}', table='{table_name}'), value=dados, overwrite=TRUE)
            dbDisconnect(con)
            print('Carga finalizada com sucesso!')
        """)

    @classmethod
    def run(cls, year_start, year_end, uf, info_sys, conn_params):
        cls.extract(year_start, year_end, uf, info_sys)

        cls.treat()

        cls.load(
            dbname=conn_params['dbname'],
            host=conn_params['host'],
            port=conn_params['port'],
            user=conn_params['user'],
            password=conn_params['password'],
            schema=conn_params['schema'],
            table_name=conn_params['table_name']
        )
