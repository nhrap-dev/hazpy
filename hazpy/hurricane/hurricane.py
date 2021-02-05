from ..common.classes import Base
from .direct_economic_loss import DirectEconomicLoss
from .direct_physical_damage import DirectPhysicalDamage
from .direct_social_loss import DirectSocialLoss
from .induced_physical_damage import InducedPhysicalDamage


class Hurricane(Base):
    """
    Intialize a hurricane module instance
     
    Keyword arguments: \n
    
    """
    def __init__(self):
        super().__init__()

        self.analysis = Analysis()

class Analysis():
    def __init__(self):
        self.directEconomicLoss = DirectEconomicLoss()
        self.directPhysicalDamage = DirectPhysicalDamage()
        self.directSocialLoss = DirectSocialLoss()
        self.inducedPhysicalDamage = InducedPhysicalDamage()