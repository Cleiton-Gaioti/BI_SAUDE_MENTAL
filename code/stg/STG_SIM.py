from STG_MODEL import Stg_Model
from rpy2.robjects import r, pandas2ri, globalenv


class Stg_SIM(Stg_Model):
    @classmethod
    def treat(cls):
        r("dados <- process_sim(dados)")

        rdf = globalenv['dados']
        pd_df = pandas2ri.rpy2py(rdf).astype(str)

        for col in pd_df.columns:
            pd_df.loc[pd_df[col] == 'NA_character_', col] = None
