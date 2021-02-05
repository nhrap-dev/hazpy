import unittest
from hazpy.tsunami import Tsunami

class TestTsunami(unittest.TestCase):
    
    def testInit(self):
        ts = Tsunami()
        self.assertEqual(type(ts), type(Tsunami()))