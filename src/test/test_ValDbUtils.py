'''
Created on Aug 19, 2012
Tests functions in ValDbUtils
@author: dpollard2
'''

import unittest
import pytz
import _mysql_exceptions as MySQLdb
import ValDbUtils as utl


class ReformatDateTest(unittest.TestCase):

    def testExactlyThreeArgs(self):
        self.assertRaises(TypeError, utl.reformatDate)
        self.assertRaises(TypeError, utl.reformatDate, 'one')
        self.assertRaises(TypeError, utl.reformatDate, 'one', 'two')
        self.assertRaises(TypeError, utl.reformatDate,'one','two','three','four')
    
    def testNoTimezoneChange(self):
        self.assertEqual(utl.reformatDate('12-Mar-10','12:00:00','UTC'),'2010-03-12 12:00')

    def testTimezoneChange(self):
        self.assertEqual(utl.reformatDate('12-Mar-10','12:00:00','America/Guyana'),'2010-03-12 16:00')

    def testBadArgFormats(self):
        self.assertRaises(ValueError,utl.reformatDate,'12-Far-10','12:00:00','UTC')
        self.assertRaises(ValueError,utl.reformatDate,'12-Xyz-10','99:00:00','UTC')
        self.assertRaises(pytz.UnknownTimeZoneError,utl.reformatDate,'12-Mar-10','12:00:00','NoWay')
        
        
class ConnectMysqlTest(unittest.TestCase):

    def testExactlyOneArg(self):
        self.assertRaises(TypeError, utl.reformatDate)
        self.assertRaises(TypeError, utl.reformatDate, 'one', 'two')
        
    def testArgType(self):
        self.assertRaises(TypeError, utl.connectMysql, 1)
        
    def testValidConnection(self):
        self.assertEqual(utl.connectMysql('TestValDb').errno(),0)
        self.assertRaises(MySQLdb.MySQLError, utl.connectMysql, 'XYZ')


class GetAssetsTest(unittest.TestCase):

    def testExactlyOneArg(self):
        self.assertRaises(TypeError, utl.getAssets)
        self.assertRaises(TypeError, utl.getAssets, 'one', 'two')
        
    def testArgType(self):
        self.assertRaises(TypeError, utl.getAssets, 1)
        
    def testInValidConnection(self):
        self.assertRaises(MySQLdb.MySQLError, utl.getAssets, 'XYZ')
        
    def testValidConnection(self):
        self.assertGreater(len(utl.getAssets('TestValDb')), 0)
        self.assertIn((1,'DDL'), utl.getAssets('TestValDb').keys())


      
class GetExchangesTest(unittest.TestCase):

    def testExactlyOneArg(self):
        self.assertRaises(TypeError, utl.getExchanges)
        self.assertRaises(TypeError, utl.getExchanges, 'one', 'two')
        
    def testArgType(self):
        self.assertRaises(TypeError, utl.getExchanges, 1)
        
    def testInValidConnection(self):
        self.assertRaises(MySQLdb.MySQLError, utl.getExchanges, 'XYZ')
        
    def testValidConnection(self):
        self.assertGreater(len(utl.getExchanges('TestValDb')), 0)
        self.assertIn('GASCI', utl.getExchanges('TestValDb').keys())
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()