import unittest
from hazpy.earthquake import Earthquake

class TestEarthquake(unittest.TestCase):

    def testInit(self):
        eq = Earthquake()
        self.assertEqual(type(eq), type(Earthquake()))