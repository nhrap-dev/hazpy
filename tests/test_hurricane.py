import unittest
from hazpy.hurricane import Hurricane

class TestEarthquake(unittest.TestCase):
    
    def testInit(self):
        hu = Hurricane()
        self.assertEqual(type(hu), type(Hurricane()))