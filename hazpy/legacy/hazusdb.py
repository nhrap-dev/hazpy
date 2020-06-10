import os
import pandas as pd
import pyodbc as py

# TODO add try execpt to all methods
"""
import hazus
db = hazus.legacy.HazusDB()
studyRegion = db.studyRegions.name[1]
hazards = db.getHazards(studyRegion)
db.getEconomicLoss(studyRegion)
db.getBuildingDamageByOccupancy(studyRegion)
db.getBuildingDamageByType(studyRegion)
db.getInjuries(studyRegion)
db.getFatalities(studyRegion)
db.getDisplacedHouseholds(studyRegion)
db.getShelterNeeds(studyRegion)
db.getDebris(studyRegion)
"""


class HazusDB():
    """Creates a connection to the Hazus SQL Server database with methods to access
    databases, tables, and study regions
    """

    def __init__(self):
        self.conn = self.createConnection()
        self.cursor = self.conn.cursor()
        self.databases = self.getDatabases()
        self.studyRegions = self.getStudyRegions()

    def createConnection(self):
        """ Creates a connection object to the local Hazus SQL Server database

            Returns:
                conn: pyodbc connection
        """
        comp_name = os.environ['COMPUTERNAME']
        conn = py.connect('Driver=ODBC Driver 11 for SQL Server;SERVER=' +
                          comp_name + '\HAZUSPLUSSRVR; UID=SA;PWD=Gohazusplus_02')
        self.conn = conn
        return conn

    def getDatabases(self):
        """Creates a dataframe of all databases in your Hazus installation

            Returns:
                df: pandas dataframe
        """
        query = 'SELECT name FROM sys.databases'
        df = pd.read_sql(query, self.conn)
        return df

    def getTables(self, databaseName):
        """Creates a dataframe of all tables in a database

            Keyword Arguments:
                databaseName: str -- the name of the Hazus SQL Server database

            Returns:
                df: pandas dataframe
        """
        query = 'SELECT * FROM [%s].INFORMATION_SCHEMA.TABLES;' % databaseName
        df = pd.read_sql(query, self.conn)
        self.tables = df
        return df

    def getStudyRegions(self):
        """Creates a dataframe of all study regions in the local Hazus SQL Server database

            Returns:
                studyRegions: pandas dataframe
        """
        exclusionRows = ['master', 'tempdb', 'model',
                         'msdb', 'syHazus', 'CDMS', 'flTmpDB']
        self.cursor.execute('SELECT [StateID] FROM [syHazus].[dbo].[syState]')
        for state in self.cursor:
            exclusionRows.append(state[0])
        query = 'SELECT * FROM sys.databases'
        df = pd.read_sql(query, self.conn)
        studyRegions = df[~df['name'].isin(exclusionRows)]['name']
        studyRegions = studyRegions.reset_index()
        studyRegions = studyRegions.drop('index', axis=1)
        self.studyRegions = studyRegions
        return studyRegions

    def query(self, sql):
        """Performs a SQL query on the Hazus SQL Server database

            Keyword Arguments:
                sql: str -- a T-SQL query

            Returns:
                df: pandas dataframe
        """
        df = pd.read_sql(sql, self.conn)
        return df

    def getHazardBoundary(self, databaseName):
        """Fetches the hazard boundary from a Hazus SQL Server database

            Keyword Arguments:
                databaseName: str -- the name of the database

            Returns:
                df: pandas dataframe -- geometry in WKT
        """
        query = 'SELECT Shape.STAsText() as geom from [%s].[dbo].[hzboundary]' % databaseName
        df = pd.read_sql(query, self.conn)
        return df

    # API new methods

    # GET
    """
    total economic loss by smallest geography
    building damage by occupancy
    building damage by structure type
    injuries and fatalities
    displaced households
    short term shelter needs
    debris
    hazard

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

    def getEconomicLoss(self, studyRegion):
        """
        returns total economic loss at the smallest geography available
        """
        hazards = self.getHazards(studyRegion)
        sql_dict = {
            'earthquake': """select Tract, SUM(ISNULL(TotalLoss, 0)) as EconLoss from {s}.dbo.[eqTractEconLoss] group by [eqTractEconLoss].Tract""".format(s=studyRegion),
            'flood': """select CensusBlock as Block, Sum(ISNULL(TotalLoss, 0)) as EconLoss from {s}.dbo.flFRGBSEcLossByTotal group by CensusBlock""".format(s=studyRegion),
            'hurricane': """select TRACT as Tract, SUM(ISNULL(TotLoss, 0) as EconLoss from {s}.dbo.[huSummaryLoss] group by Tract""".format(s=studyRegion),
            'tsunami': """select CensusBlock as Block, SUM(ISNULL(TotalLoss, 0)) as EconLoss from {s}.dbo.tsuvResDelKTotB group by CensusBlock""".format(s=studyRegion)
        }

        df = self.query(sql_dict[hazards[0]])
        return df

    def getBuildingDamageByOccupancy(self, studyRegion):
        """ returns building damage by occupancy type
        """
        hazards = self.getHazards(studyRegion)
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

    def getBuildingDamageByType(self, studyRegion):
        """ returns building damage by structure type
        """
        hazards = self.getHazards(studyRegion)
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

    def getInjuries(self, studyRegion):
        """ returns injuries
        """
        hazards = self.getHazards(studyRegion)
        sql_dict = {
            'earthquake': """SELECT Tract, SUM(CASE WHEN CasTime = 'N' THEN Level1Injury 
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
                cdf.CensusBlock,
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

    def getFatalities(self, studyRegion):
        """ returns fatalities
        """
        hazards = self.getHazards(studyRegion)
        sql_dict = {
            'earthquake': """SELECT Tract, SUM(CASE WHEN CasTime = 'N' 
                THEN Level4Injury ELSE 0 End) AS Night, SUM(CASE WHEN CasTime = 'D' 
                THEN Level4Injury ELSE 0 End) AS Day FROM eq_test_AK.dbo.[eqTractCasOccup] 
                WHERE CasTime IN ('N', 'D') AND InOutTot = 'Tot' GROUP BY Tract""".format(s=studyRegion),
            'flood': """ """.format(s=studyRegion),
            'hurricane': """ """.format(s=studyRegion),
            'tsunami': """SELECT
                cdf.CensusBlock,
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

    def getDisplacedHouseholds(self, studyRegion):
        """ returns displaced households
        """
        hazards = self.getHazards(studyRegion)
        # TODO check to see if flood is displaced households or population -- database says pop
        sql_dict = {
            'earthquake': """select Tract, SUM(DisplacedHouseholds) as DisplacedHouseholds from {s}.dbo.eqTract group by Tract""".format(s=studyRegion),
            'flood': """select CensusBlock as Block, SUM(DisplacedPop) as DisplacedHouseholds from {s}.dbo.flFRShelter group by CensusBlock""".format(s=studyRegion),
            'hurricane': """select TRACT as Tract, SUM(DISPLACEDHOUSEHOLDS) as DisplacedHouseholds from {s}.dbo.huShelterResultsT group by Tract""".format(s=studyRegion),
            'tsunami': """ """.format(s=studyRegion)
        }

        df = self.query(sql_dict[hazards[0]])
        return df

    def getShelterNeeds(self, studyRegion):
        """ returns shelter needs
        """
        hazards = self.getHazards(studyRegion)
        sql_dict = {
            'earthquake': """select Tract, SUM(ShortTermShelter) as ShelterNeeds from {s}.dbo.eqTract group by Tract""".format(s=studyRegion),
            'flood': """select CensusBlock as Block, SUM(ShortTermNeeds) as ShelterNeeds from {s}.dbo.flFRShelter group by CensusBlock""".format(s=studyRegion),
            'hurricane': """select TRACT as Tract, SUM(SHORTTERMSHELTERNEEDS) as ShelterNeeds from {s}.dbo.huShelterResultsT group by Tract
                """.format(s=studyRegion),
            'tsunami': """ """.format(s=studyRegion)
        }

        df = self.query(sql_dict[hazards[0]])
        return df

    def getDebris(self, studyRegion):
        """ returns debris
        """
        hazards = self.getHazards(studyRegion)
        sql_dict = {
            'earthquake': """select Tract, SUM(DebrisW) as DebrisBW, SUM(DebrisS) as DebrisCS, SUM(DebrisTotal) as DebrisTotal from {s}.dbo.eqTract group by Tract""".format(s=studyRegion),
            'flood': """select CensusBlock as Block, (SUM(FinishTons) * 2000) as DebrisTotal from {s}.dbo.flFRDebris group by CensusBlock""".format(s=studyRegion),
            'hurricane': """select Tract, SUM(BRICKANDWOOD) as DebrisBW, SUM(CONCRETEANDSTEEL) as DebrisCS, SUM(Tree) as DebrisTree, SUM(BRICKANDWOOD + CONCRETEANDSTEEL + Tree) as DebrisTotal from {s}.dbo.huDebrisResultsT group by Tract""".format(s=studyRegion),
            'tsunami': """ """.format(s=studyRegion)
        }

        df = self.query(sql_dict[hazards[0]])
        return df

    def getHazardBoundary():
        """ returns hazard
        """
        hazards = self.getHazards(studyRegion)
        sql_dict = {
            'earthquake': """ """.format(s=studyRegion),
            'flood': """ """.format(s=studyRegion),
            'hurricane': """ """.format(s=studyRegion),
            'tsunami': """ """.format(s=studyRegion)
        }

        df = self.query(sql_dict[hazards[0]])
        return df

    def getHazards(self, studyRegion, returnType='list'):
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
            hazardsList = list(filter(lambda x: hazardsDict[x], hazardsDict))
            return hazardsList
