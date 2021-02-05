from ..common.classes import Base
from .general_building_stock import GeneralBuildingStock
from .udf import UDF

class Tsunami(Base):
    """
    Intialize a tsunami module instance
     
    Keyword arguments: \n

    """
    def __init__(self):
        super().__init__()

        self.analysis = Analysis()

class Analysis():
    def __init__(self):
        self.generalBuildingStock = GeneralBuildingStock()
        self.UDF = UDF()