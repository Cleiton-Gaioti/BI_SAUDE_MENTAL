from rpy2.robjects import r
from STG_MODEL import Stg_Model


class Stg_SIM(Stg_Model):
    @classmethod
    def treat(cls):
        r("dados <- process_sim(dados)")
