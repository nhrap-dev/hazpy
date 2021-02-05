import unittest
from hazpy.tornado import Tornado

class TestTornado(unittest.TestCase):
    
    def testInit(self):
        tn = Tornado()
        self.assertEqual(type(tn), type(Tornado()))