import unittest
from hazpy.flood import Flood
from hazpy.flood import UDF

class TestFlood(unittest.TestCase):

    def testInit(self):
        flood = Flood()
        self.assertEqual(type(flood), type(Flood()))

    def testUDFInit(self):
        udf = UDF()
        self.assertEqual(type(udf), type(UDF()))

    def testUDFLocal(self):
        # Unable to test because it requires lookup tables that are only in the GUI
        # TODO move method to the GUI because hazpy cannot perform it on it's own
        pass


