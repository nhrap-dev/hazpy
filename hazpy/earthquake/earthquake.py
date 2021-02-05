from ..common.classes import Base
from .aebm import AEBM
from .direct_social_losses import DirectSocialLosses
from .essential_facilities import EssentialFacilities
from .general_buildings import GeneralBuildings
from .indirect_economic_loss import IndirectEconomicLoss
from .induced_physical_damage import InducedPhysicalDamage
from .military_installation import MilitaryInstallation
from .transportation_systems import TransportationSystems
from .udf import UDF
from .utility_systems import UtilitySystems


class Earthquake(Base):
    """Intialize an earthquake module instance.
     
    Keyword arguments:
    """
    def __init__(self):
        super().__init__()

        self.analysis = Analysis()

class Analysis():
    def __init__(self):
        self.AEBM = AEBM()
        self.directSocialLosses = DirectSocialLosses()
        self.essentialFacilities = EssentialFacilities()
        self.generalBuildings = GeneralBuildings()
        self.indirectEconomicLoss = IndirectEconomicLoss()
        self.inducedPhysicalDamage = InducedPhysicalDamage()
        self.militaryInstallation = MilitaryInstallation()
        self.transportationSystems = TransportationSystems()
        self.UDF = UDF()
        self.utilitySystems = UtilitySystems()
