import os
import pandas as pd
import pyodbc as py
from sqlalchemy import create_engine
import sys

""" testing -------------
import hazpy
db = hazpy.legacy.HazusDB()
db.getStudyRegions()
studyRegion = db.studyRegions.name[4]
db.getHazard(studyRegion)
hazards = db.getHazardsAnalyzed(studyRegion)
db.getHazardBoundary(studyRegion)
el = db.getEconomicLoss(studyRegion)
geomdf = db.joinGeometry(el, studyRegion)
db.getBuildingDamageByOccupancy(studyRegion)
db.getBuildingDamageByType(studyRegion)
db.getInjuries(studyRegion)
db.getFatalities(studyRegion)
db.getDisplacedHouseholds(studyRegion)
db.getShelterNeeds(studyRegion)
db.getDebris(studyRegion)

"""
# API new methods

# GET
"""
TS
    travel time to safety
    water depth
EQ
    inspected, restricted, unsafe
    PGA by tract
HU
    damaged essential facilities
    peak gust by track
"""


class HazusDB():
    """Creates a connection to the Hazus SQL Server database with methods to access
    databases, tables, and study regions
    """

    def __init__(self):
        self.conn = self.createConnection()
        # self.cursor = self.conn.cursor()
        self.databases = self.getDatabases()
        self.studyRegions = self.getStudyRegions()

    def createConnection(self, orm='pyodbc'):
        """ Creates a connection object to the local Hazus SQL Server database

            Key Argument:
                orm: string -- type of connection to return (choices: 'pyodbc', 'sqlalchemy')
            Returns:
                conn: pyodbc connection
        """
        try:
            comp_name = os.environ['COMPUTERNAME']
            if orm == 'pyodbc':
                conn = py.connect('Driver=ODBC Driver 11 for SQL Server;SERVER=' +
                                  comp_name + '\HAZUSPLUSSRVR; UID=SA;PWD=Gohazusplus_02')
            # TODO add sqlalchemy connection
            # if orm == 'sqlalchemy':
            #     conn = create_engine('mssql+pyodbc://SA:Gohazusplus_02@HAZUSPLUSSRVR')
            # self.conn = conn
            return conn
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getDatabases(self):
        """Creates a dataframe of all databases in your Hazus installation

            Returns:
                df: pandas dataframe
        """
        try:
            query = 'SELECT name FROM sys.databases'
            df = pd.read_sql(query, self.conn)
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getTables(self, databaseName):
        """Creates a dataframe of all tables in a database

            Keyword Arguments:
                databaseName: str -- the name of the Hazus SQL Server database

            Returns:
                df: pandas dataframe
        """
        try:
            query = 'SELECT * FROM [%s].INFORMATION_SCHEMA.TABLES;' % databaseName
            df = pd.read_sql(query, self.conn)
            self.tables = df
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getStudyRegions(self):
        """Creates a dataframe of all study regions in the local Hazus SQL Server database

            Returns:
                studyRegions: pandas dataframe
        """
        try:
            exclusionRows = ['master', 'tempdb', 'model',
                             'msdb', 'syHazus', 'CDMS', 'flTmpDB']
            sql = 'SELECT [StateID] FROM [syHazus].[dbo].[syState]'
            queryset = self.query(sql)
            states = list(queryset['StateID'])
            for state in states:
                exclusionRows.append(state)
            sql = 'SELECT * FROM sys.databases'
            df = self.query(sql)
            studyRegions = df[~df['name'].isin(exclusionRows)]['name']
            studyRegions = studyRegions.reset_index()
            studyRegions = studyRegions.drop('index', axis=1)
            self.studyRegions = studyRegions
            return studyRegions
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def query(self, sql):
        """Performs a SQL query on the Hazus SQL Server database

            Keyword Arguments:
                sql: str -- a T-SQL query

            Returns:
                df: pandas dataframe
        """
        try:
            df = pd.read_sql(sql, self.conn)
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
