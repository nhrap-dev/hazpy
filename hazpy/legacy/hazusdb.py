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

    def getHazardBoundary(self, studyRegion):
        """Fetches the hazard boundary from a Hazus SQL Server database

            Keyword Arguments:
                databaseName: str -- the name of the database

            Returns:
                df: pandas dataframe -- geometry in WKT
        """
        try:
            sql = 'SELECT Shape.STAsText() as geom from [%s].[dbo].[hzboundary]' % studyRegion
            df = self.query(sql)
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getEconomicLoss(self, studyRegion):
        """
        Queries the total economic loss for a study region from the local Hazus SQL Server database

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of economic loss
        """
        try:
            hazards = self.getHazardsAnalyzed(studyRegion)
            sql_dict = {
                'earthquake': """select Tract as tract, SUM(ISNULL(TotalLoss, 0)) as EconLoss from {s}.dbo.[eqTractEconLoss] group by [eqTractEconLoss].Tract""".format(s=studyRegion),
                'flood': """select CensusBlock as block, Sum(ISNULL(TotalLoss, 0)) as EconLoss from {s}.dbo.flFRGBSEcLossByTotal group by CensusBlock""".format(s=studyRegion),
                'hurricane': """select TRACT as tract, SUM(ISNULL(TotLoss, 0)) as EconLoss from {s}.dbo.[huSummaryLoss] group by Tract""".format(s=studyRegion),
                'tsunami': """select CensusBlock as block, SUM(ISNULL(TotalLoss, 0)) as EconLoss from {s}.dbo.tsuvResDelKTotB group by CensusBlock""".format(s=studyRegion)
            }

            df = self.query(sql_dict[hazards[0]])
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getBuildingDamageByOccupancy(self, studyRegion):
        """ Queries the building damage by occupancy type for a study region from the local Hazus SQL Server database

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of building damage by occupancy type
        """
        try:
            hazards = self.getHazardsAnalyzed(studyRegion)
            sql_dict = {
                'earthquake': """SELECT Occupancy, SUM(ISNULL(PDsNoneBC, 0))
                    As NoDamage, SUM(ISNULL(PDsSlightBC, 0)) AS Affected, SUM(ISNULL(PDsModerateBC, 0))
                    AS Minor, SUM(ISNULL(PDsExtensiveBC, 0)) AS Major,
                    SUM(ISNULL(PDsCompleteBC, 0)) AS Destroyed FROM {s}.dbo.[eqTractDmg]
                    WHERE DmgMechType = 'STR' GROUP BY Occupancy""".format(s=studyRegion),
                'flood': """SELECT SOccup AS Occupancy, SUM(ISNULL(TotalLoss, 0))
                    AS TotalLoss, SUM(ISNULL(BuildingLoss, 0)) AS BldgLoss,
                    SUM(ISNULL(ContentsLoss, 0)) AS ContLoss
                    FROM {s}.dbo.[flFRGBSEcLossBySOccup] GROUP BY SOccup""".format(s=studyRegion),
                'hurricane': """SELECT GenBldgOrGenOcc AS Occupancy,
                    SUM(ISNULL(NonDamage, 0)) As NoDamage, SUM(ISNULL(MinDamage, 0)) AS Affected,
                    SUM(ISNULL(ModDamage, 0)) AS Minor, SUM(ISNULL(SevDamage, 0)) AS Major,
                    SUM(ISNULL(ComDamage, 0)) AS Destroyed FROM {s}.dbo.[huSummaryDamage]
                    WHERE GenBldgOrGenOcc IN('COM', 'AGR', 'GOV', 'EDU', 'REL','RES', 'IND')
                    GROUP BY GenBldgOrGenOcc""".format(s=studyRegion),
                'tsunami': """SELECT LEFT({s}.dbo.tsHazNsiGbs.NsiID, 3) As Occupancy,
                    COUNT({s}.dbo.tsHazNsiGbs.NsiID) As Total,
                    COUNT(CASE WHEN {s}.dbo.tsHazNsiGbs.ValStruct > 0 AND {s}.dbo.tsHazNsiGbs.ValCont > 0 AND
                    (BldgLoss/({s}.dbo.tsHazNsiGbs.ValStruct+{s}.dbo.tsHazNsiGbs.ValCont))
                    <= 0.05 THEN 1 ELSE NULL END) As Affected,
                    COUNT(CASE WHEN {s}.dbo.tsHazNsiGbs.ValStruct > 0 AND {s}.dbo.tsHazNsiGbs.ValCont > 0 AND
                    (BldgLoss/({s}.dbo.tsHazNsiGbs.ValStruct+{s}.dbo.tsHazNsiGbs.ValCont))
                    > 0.05 AND (BldgLoss/({s}.dbo.tsHazNsiGbs.ValStruct+{s}.dbo.tsHazNsiGbs.ValCont))
                    <= 0.3 THEN 1 ELSE NULL END) As Minor,
                    COUNT(CASE WHEN {s}.dbo.tsHazNsiGbs.ValStruct > 0 AND {s}.dbo.tsHazNsiGbs.ValCont > 0 AND
                    (BldgLoss/({s}.dbo.tsHazNsiGbs.ValStruct+{s}.dbo.tsHazNsiGbs.ValCont))
                    > 0.3 AND (BldgLoss/({s}.dbo.tsHazNsiGbs.ValStruct+{s}.dbo.tsHazNsiGbs.ValCont))
                    <= 0.5 THEN 1 ELSE NULL END) As Major,
                    COUNT(CASE WHEN {s}.dbo.tsHazNsiGbs.ValStruct > 0 AND {s}.dbo.tsHazNsiGbs.ValCont > 0 AND
                    (BldgLoss/({s}.dbo.tsHazNsiGbs.ValStruct+{s}.dbo.tsHazNsiGbs.ValCont))
                    > 0.5 THEN 1 ELSE NULL END) As Destroyed
                    FROM {s}.dbo.tsHazNsiGbs FULL JOIN {s}.dbo.tsNsiGbs
                    ON {s}.dbo.tsHazNsiGbs.NsiID = {s}.dbo.tsNsiGbs.NsiID
                    FULL JOIN [{s}].[dbo].[tsFRNsiGbs] ON {s}.dbo.tsNsiGbs.NsiID =
                    [{s}].[dbo].[tsFRNsiGbs].NsiID WHERE {s}.dbo.tsHazNsiGbs.NsiID IS NOT NULL
                    GROUP BY LEFT({s}.dbo.tsHazNsiGbs.NsiID, 3)""".format(s=studyRegion)
            }

            df = self.query(sql_dict[hazards[0]])
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getBuildingDamageByType(self, studyRegion):
        """ Queries the building damage by structure type for a study region from the local Hazus SQL Server database

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of building damage by structure type
        """
        try:
            hazards = self.getHazardsAnalyzed(studyRegion)
            sql_dict = {
                'earthquake': """SELECT eqBldgType AS BldgType,
                    SUM(ISNULL(PDsNoneBC, 0)) As NoDamage, SUM(ISNULL(PDsSlightBC, 0)) AS Affected,
                    SUM(ISNULL(PDsModerateBC, 0)) AS Minor, SUM(ISNULL(PDsExtensiveBC, 0))
                    AS Major, SUM(ISNULL(PDsCompleteBC, 0)) AS Destroyed
                    FROM {s}.dbo.[eqTractDmg] WHERE DmgMechType = 'STR'
                    GROUP BY eqBldgType""".format(s=studyRegion),
                'flood': """SELECT BldgType, SUM(ISNULL(TotalLoss, 0)) AS TotalLoss,
                    SUM(ISNULL(BuildingLoss, 0)) AS BldgLoss, SUM(ISNULL(ContentsLoss, 0)) AS ContLoss
                    FROM {s}.dbo.[flFRGBSEcLossByGBldgType] GROUP BY BldgType""".format(s=studyRegion),
                'hurricane': """SELECT GenBldgOrGenOcc AS Occupancy,
                    SUM(ISNULL(NonDamage, 0)) As NoDamage, SUM(ISNULL(MinDamage, 0)) AS Affected,
                    SUM(ISNULL(ModDamage, 0)) AS Minor, SUM(ISNULL(SevDamage, 0)) AS Major,
                    SUM(ISNULL(ComDamage, 0)) AS Destroyed FROM {s}.dbo.[huSummaryDamage]
                    WHERE GenBldgOrGenOcc IN('CONCRETE', 'MASONRY', 'STEEL', 'WOOD', 'MH')
                    GROUP BY GenBldgOrGenOcc""".format(s=studyRegion),
                'tsunami': """SELECT eqBldgType AS BldgType, [Description],
                    COUNT({s}.dbo.tsHazNsiGbs.NsiID) As Structures,
                    COUNT(CASE WHEN {s}.dbo.tsHazNsiGbs.ValStruct > 0 AND {s}.dbo.tsHazNsiGbs.ValCont > 0 AND
                    (BldgLoss/({s}.dbo.tsHazNsiGbs.ValStruct+{s}.dbo.tsHazNsiGbs.ValCont))
                    <= 0.05 THEN 1 ELSE NULL END) As Affected,
                    COUNT(CASE WHEN {s}.dbo.tsHazNsiGbs.ValStruct > 0 AND {s}.dbo.tsHazNsiGbs.ValCont > 0 AND
                    (BldgLoss/({s}.dbo.tsHazNsiGbs.ValStruct+{s}.dbo.tsHazNsiGbs.ValCont))
                    > 0.05 AND (BldgLoss/({s}.dbo.tsHazNsiGbs.ValStruct+{s}.dbo.tsHazNsiGbs.ValCont))
                    <= 0.3 THEN 1 ELSE NULL END) As Minor,
                    COUNT(CASE WHEN {s}.dbo.tsHazNsiGbs.ValStruct > 0 AND {s}.dbo.tsHazNsiGbs.ValCont > 0 AND
                    (BldgLoss/({s}.dbo.tsHazNsiGbs.ValStruct+{s}.dbo.tsHazNsiGbs.ValCont))
                    > 0.3 AND (BldgLoss/({s}.dbo.tsHazNsiGbs.ValStruct+{s}.dbo.tsHazNsiGbs.ValCont))
                    <= 0.5 THEN 1 ELSE NULL END) As Major,
                    COUNT(CASE WHEN {s}.dbo.tsHazNsiGbs.ValStruct > 0 AND {s}.dbo.tsHazNsiGbs.ValCont > 0 AND
                    (BldgLoss/({s}.dbo.tsHazNsiGbs.ValStruct+{s}.dbo.tsHazNsiGbs.ValCont))
                    > 0.5 THEN 1 ELSE NULL END) As Destroyed
                    FROM {s}.dbo.tsHazNsiGbs FULL JOIN {s}.dbo.eqclBldgType
                    ON {s}.dbo.tsHazNsiGbs.EqBldgTypeID = {s}.dbo.eqclBldgType.DisplayOrder
                    FULL JOIN [{s}].[dbo].[tsFRNsiGbs] ON {s}.dbo.tsHazNsiGbs.NsiID =
                    [{s}].[dbo].[tsFRNsiGbs].NsiID WHERE EqBldgTypeID IS NOT NULL
                    GROUP BY eqBldgType, [Description]""".format(s=studyRegion)
            }

            df = self.query(sql_dict[hazards[0]])
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getInjuries(self, studyRegion):
        """ Queries the injuries for a study region from the local Hazus SQL Server database

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of injuries
        """
        try:
            hazards = self.getHazardsAnalyzed(studyRegion)
            sql_dict = {
                'earthquake': """SELECT Tract as tract, SUM(CASE WHEN CasTime = 'N' THEN Level1Injury 
                    ELSE 0 END) AS NightLevel1, SUM(CASE WHEN CasTime = 'N' 
                    THEN Level2Injury ELSE 0 END) AS NightLevel2, SUM(CASE WHEN CasTime = 'N' 
                    THEN Level3Injury ELSE 0 END) AS NiteLevel3, SUM(CASE WHEN CasTime = 'N' 
                    THEN Level1Injury ELSE 0 END) AS DayLevel1,  SUM(CASE WHEN CasTime = 'D' 
                    THEN Level2Injury ELSE 0 END) AS DayLevel2, SUM(CASE WHEN CasTime = 'D' 
                    THEN Level3Injury ELSE 0 END) AS DayLevel3 FROM {s}.dbo.[eqTractCasOccup] 
                    WHERE CasTime IN ('N', 'D') AND InOutTot = 'Tot' GROUP BY Tract""".format(s=studyRegion),
                'flood': """ """.format(s=studyRegion),
                'hurricane': """ """.format(s=studyRegion),
                'tsunami': """SELECT
                    cdf.CensusBlock as block,
                    SUM(cdf.InjuryDayTotal) as InjuryDayFair,
                    SUM(cdg.InjuryDayTotal) As InjuryDayGood,
                    SUM(cdp.InjuryDayTotal) As InjuryDayPoor,
                    SUM(cnf.InjuryNightTotal) As InjuryNightFair,
                    SUM(cng.InjuryNightTotal) As InjuryNightGood,
                    SUM(cnp.InjuryNightTotal) As InjuryNightPoor
                        FROM {s}.dbo.tsCasualtyDayFair as cdf
                            FULL JOIN {s}.dbo.tsCasualtyDayGood as cdg
                                ON cdf.CensusBlock = cdg.CensusBlock
                            FULL JOIN {s}.dbo.tsCasualtyDayPoor as cdp
                                ON cdf.CensusBlock = cdp.CensusBlock
                            FULL JOIN {s}.dbo.tsCasualtyNightFair as cnf
                                ON cdf.CensusBlock = cnf.CensusBlock
                            FULL JOIN {s}.dbo.tsCasualtyNightGood as cng
                                ON cdf.CensusBlock = cng.CensusBlock
                            FULL JOIN {s}.dbo.tsCasualtyNightPoor as cnp
                                ON cdf.CensusBlock = cnp.CensusBlock
                            group by cdf.CensusBlock""".format(s=studyRegion)
            }

            df = self.query(sql_dict[hazards[0]])
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getFatalities(self, studyRegion):
        """ Queries the fatalities for a study region from the local Hazus SQL Server database

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of fatalities
        """
        try:
            hazards = self.getHazardsAnalyzed(studyRegion)
            sql_dict = {
                'earthquake': """SELECT Tract as tract, SUM(CASE WHEN CasTime = 'N' 
                    THEN Level4Injury ELSE 0 End) AS Night, SUM(CASE WHEN CasTime = 'D' 
                    THEN Level4Injury ELSE 0 End) AS Day FROM eq_test_AK.dbo.[eqTractCasOccup] 
                    WHERE CasTime IN ('N', 'D') AND InOutTot = 'Tot' GROUP BY Tract""".format(s=studyRegion),
                'flood': """ """.format(s=studyRegion),
                'hurricane': """ """.format(s=studyRegion),
                'tsunami': """SELECT
                    cdf.CensusBlock as block,
                    SUM(cdf.FatalityDayTotal) As FatalityDayFair,
                    SUM(cdg.FatalityDayTotal) As FatalityDayGood,
                    SUM(cdp.FatalityDayTotal) As FatalityDayPoor,
                    SUM(cnf.FatalityNightTotal) As FatalityNightFair,
                    SUM(cng.FatalityNightTotal) As FatalityNightGood,
                    SUM(cnp.FatalityNightTotal) As FatalityNightPoor
                        FROM ts_test.dbo.tsCasualtyDayFair as cdf
                            FULL JOIN ts_test.dbo.tsCasualtyDayGood as cdg
                                ON cdf.CensusBlock = cdg.CensusBlock
                            FULL JOIN ts_test.dbo.tsCasualtyDayPoor as cdp
                                ON cdf.CensusBlock = cdp.CensusBlock
                            FULL JOIN ts_test.dbo.tsCasualtyNightFair as cnf
                                ON cdf.CensusBlock = cnf.CensusBlock
                            FULL JOIN ts_test.dbo.tsCasualtyNightGood as cng
                                ON cdf.CensusBlock = cng.CensusBlock
                            FULL JOIN ts_test.dbo.tsCasualtyNightPoor as cnp
                                ON cdf.CensusBlock = cnp.CensusBlock
                            group by cdf.CensusBlock""".format(s=studyRegion)
            }

            df = self.query(sql_dict[hazards[0]])
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getDisplacedHouseholds(self, studyRegion):
        """ Queries the displaced households for a study region from the local Hazus SQL Server database

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of displaced households
        """
        try:
            hazards = self.getHazardsAnalyzed(studyRegion)
            # TODO check to see if flood is displaced households or population -- database says pop
            sql_dict = {
                'earthquake': """select Tract as tract, SUM(DisplacedHouseholds) as DisplacedHouseholds from {s}.dbo.eqTract group by Tract""".format(s=studyRegion),
                'flood': """select CensusBlock as block, SUM(DisplacedPop) as DisplacedHouseholds from {s}.dbo.flFRShelter group by CensusBlock""".format(s=studyRegion),
                'hurricane': """select TRACT as tract, SUM(DISPLACEDHOUSEHOLDS) as DisplacedHouseholds from {s}.dbo.huShelterResultsT group by Tract""".format(s=studyRegion),
                'tsunami': """ """.format(s=studyRegion)
            }

            df = self.query(sql_dict[hazards[0]])
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getShelterNeeds(self, studyRegion):
        """ Queries the short term shelter needs for a study region from the local Hazus SQL Server database

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of short term shelter needs
        """
        try:
            hazards = self.getHazardsAnalyzed(studyRegion)
            sql_dict = {
                'earthquake': """select Tract as tract, SUM(ShortTermShelter) as ShelterNeeds from {s}.dbo.eqTract group by Tract""".format(s=studyRegion),
                'flood': """select CensusBlock as block, SUM(ShortTermNeeds) as ShelterNeeds from {s}.dbo.flFRShelter group by CensusBlock""".format(s=studyRegion),
                'hurricane': """select TRACT as tract, SUM(SHORTTERMSHELTERNEEDS) as ShelterNeeds from {s}.dbo.huShelterResultsT group by Tract
                    """.format(s=studyRegion),
                'tsunami': """ """.format(s=studyRegion)
            }

            df = self.query(sql_dict[hazards[0]])
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getDebris(self, studyRegion):
        """ Queries the debris for a study region from the local Hazus SQL Server database

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of debris
        """
        try:
            hazards = self.getHazardsAnalyzed(studyRegion)
            sql_dict = {
                'earthquake': """select Tract as tract, SUM(DebrisW) as DebrisBW, SUM(DebrisS) as DebrisCS, SUM(DebrisTotal) as DebrisTotal from {s}.dbo.eqTract group by Tract""".format(s=studyRegion),
                'flood': """select CensusBlock as block, (SUM(FinishTons) * 2000) as DebrisTotal from {s}.dbo.flFRDebris group by CensusBlock""".format(s=studyRegion),
                'hurricane': """select Tract as tract, SUM(BRICKANDWOOD) as DebrisBW, SUM(CONCRETEANDSTEEL) as DebrisCS, SUM(Tree) as DebrisTree, SUM(BRICKANDWOOD + CONCRETEANDSTEEL + Tree) as DebrisTotal from {s}.dbo.huDebrisResultsT group by Tract""".format(s=studyRegion),
                'tsunami': """ """.format(s=studyRegion)
            }

            df = self.query(sql_dict[hazards[0]])
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getHazardsAnalyzed(self, studyRegion, returnType='list'):
        """ Queries the local Hazus SQL Server database and returns all hazards analyzed

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
                returnType: string -- choices: 'list', 'dict'
            Returns:
                df: pandas dataframe -- a dataframe of the hazards analyzed
        """
        try:
            sql = "select * from [syHazus].[dbo].[syStudyRegion] where [RegionName] = '" + \
                studyRegion + "'"
            df = self.query(sql)
            hazardsDict = {
                'earthquake': df['HasEqHazard'][0],
                'hurricane': df['HasHuHazard'][0],
                'tsunami': df['HasTsHazard'][0],
                'flood': df['HasFlHazard'][0]
            }
            if returnType == 'dict':
                return hazardsDict
            if returnType == 'list':
                hazardsList = list(
                    filter(lambda x: hazardsDict[x], hazardsDict))
                return hazardsList
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getHazard(self, studyRegion):
        """ Queries the local Hazus SQL Server database and returns a geodataframe of the hazard

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of the hazard
        """
        try:
            hazards = self.getHazardsAnalyzed(studyRegion)
            sql_dict = {
                'earthquake': """SELECT Tract as tract, PGA from {s}.dbo.[eqTract]""".format(s=studyRegion),
                'flood': """ """.format(s=studyRegion),
                'hurricane': """ """.format(s=studyRegion),
                'tsunami': """ """.format(s=studyRegion)
            }

            df = self.query(sql_dict[hazards[0]])
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getCensusTracts(self, studyRegion):
        """ Queries the census tract geometry for a study region in local Hazus SQL Server database

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of the census geometry and fips codes
        """
        try:
            hazards = self.getHazardsAnalyzed(studyRegion)
            sql = """SELECT Tract as tract, Shape.STAsText() AS Shape FROM {s}.dbo.hzTract""".format(
                s=studyRegion)

            df = self.query(sql)
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getCensusBlocks(self, studyRegion):
        """ Queries the census block geometry for a study region in local Hazus SQL Server database

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of the census geometry and fips codes
        """
        try:
            hazards = self.getHazardsAnalyzed(studyRegion)
            sql = """SELECT CensusBlock as block, Shape.STAsText() AS Shape FROM {s}.dbo.hzCensusBlock""".format(
                s=studyRegion)

            df = self.query(sql)
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getCounties(self, studyRegion):
        """ Queries the county geometry for a study region in local Hazus SQL Server database

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of the census geometry and fips codes
        """
        try:
            hazards = self.getHazardsAnalyzed(studyRegion)
            sql = """SELECT CountyFips as county, CountyName as name, Shape.STAsText() AS Shape FROM {s}.dbo.hzCounty""".format(
                s=studyRegion)

            df = self.query(sql)
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getEssentialFacilities():
        """ Queries the call essential facilities for a study region in local Hazus SQL Server database

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
            Returns:
                df: pandas dataframe -- a dataframe of the essential facilities and damages
        """
        try:
            essential_facilities = ['CareFlty', 'EmergencyCtr', 'FireStation',
                                    'PoliceStation', 'School', 'AirportFlty', 'BusFlty', 'FerryFlty',
                                    'HighwayBridge', 'HighwayTunnel', 'LightRailBridge', 'LightRailFlty',
                                    'LightRailTunnel', 'PortFlty', 'RailFlty', 'RailwayBridge',
                                    'RailwayTunnel', 'Runway', 'ElectricPowerFlty', 'CommunicationFlty',
                                    'NaturalGasFlty', 'OilFlty', 'PotableWaterFlty', 'WasteWaterFlty',
                                    'Dams', 'Military', 'NuclearFlty', 'HighwaySegment', 'LightRailSegment',
                                    'RailwaySegment', 'NaturalGasPl', 'OilPl', 'WasteWaterPl', 'Levees']

            hazards = self.getHazardsAnalyzed(studyRegion)
            # sql = """SELECT CountyFips as county, CountyName as name, Shape.STAsText() AS Shape FROM {s}.dbo.hzCounty""".format(s=studyRegion)

            df = self.query(sql)
            return df
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def joinGeometry(self, dataframe, studyRegion):
        """ Adds geometry to any HazusDB class dataframe with a census block, tract, or county id

            Key Argument:
                studyRegion: string -- the name of the Hazus study region
                dataframe: pandas dataframe -- a HazusDB generated dataframe with a fips column named either block, tract, or county
            Returns:
                df: pandas dataframe -- a copy of the input dataframe with the geometry added
        """
        try:
            if 'block' in dataframe.columns:
                geomdf = self.getCensusBlocks(studyRegion)
                df = geomdf.merge(dataframe, on='block', how='inner')
                df.columns = [
                    x if x != 'Shape' else 'geometry' for x in df.columns]
                return df
            elif 'tract' in dataframe.columns:
                geomdf = self.getCensusTracts(studyRegion)
                df = geomdf.merge(dataframe, on='tract', how='inner')
                df.columns = [
                    x if x != 'Shape' else 'geometry' for x in df.columns]
                return df
            elif 'county' in dataframe.columns:
                geomdf = self.getCounties(studyRegion)
                df = geomdf.merge(dataframe, on='county', how='inner')
                df.columns = [
                    x if x != 'Shape' else 'geometry' for x in df.columns]
                return df
            else:
                print(
                    'Unable to find the column name block, tract, or county in the dataframe input')
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
