import os
import pandas as pd
import geopandas as gpd
import pyodbc as py
from shapely.wkt import loads
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
import urllib
# TODO check if all geojsons are oriented correctly; if not, apply orient
# try:
#     from shapely.ops import orient  # version >=1.7a2
# except:
#     from shapely.geometry.polygon import orient
from sqlalchemy import create_engine
import sys
from functools import reduce

import rasterio as rio
from rasterio import features
import numpy as np

from .studyregiondataframe import StudyRegionDataFrame
from .report import Report


class StudyRegion():
    """ Creates a study region object using an existing study region in the local Hazus database

        Keyword Arguments:
            studyRegion: str -- the name of the study region
    """

    def __init__(self, studyRegion):
        self.name = studyRegion
        self.conn = self.createConnection()
        self.hazards = self.getHazardsAnalyzed()
        self.conn = self.createConnection()
        self.report = Report(self, self.name, '', self.hazards[0])

    def createConnection(self):
        """ Creates a connection object to the local Hazus SQL Server database

            Key Argument:
                orm: string -- type of connection to return (choices: 'pyodbc', 'sqlalchemy')
            Returns:
                conn: pyodbc connection
        """
        try:
            comp_name = os.environ['COMPUTERNAME']
            server = comp_name+"\HAZUSPLUSSRVR"
            user = 'SA'
            password = 'Gohazusplus_02'
            driver = 'ODBC Driver 13 for SQL Server'
            # driver = 'ODBC Driver 11 for SQL Server'
            engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(urllib.parse.quote_plus(
                "DRIVER={0};SERVER={1};PORT=1433;DATABASE={2};UID={3};PWD={4};TDS_Version=8.0;".format(driver, server, self.name, user, password))))
            conn = engine.connect()
            return conn
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
            return StudyRegionDataFrame(self, df)
        except:
            # uncomment error print only for debugging
            # print("Unexpected error:", sys.exc_info()[0])
            raise

    def getHazardBoundary(self):
        """Fetches the hazard boundary from a Hazus SQL Server database

            Returns:
                df: pandas dataframe -- geometry in WKT
        """
        try:
            sql = 'SELECT Shape.STAsText() as geom from [%s].[dbo].[hzboundary]' % self.name
            df = self.query(sql)
            return StudyRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getEconomicLoss(self):
        """
        Queries the total economic loss for a study region from the local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of economic loss
        """
        try:
            # constant to convert to real USD
            constant = 1000
            sqlDict = {
                'earthquake': """select Tract as tract, SUM(ISNULL(TotalLoss, 0)) * {c} as EconLoss from {s}.dbo.[eqTractEconLoss] group by [eqTractEconLoss].Tract""".format(s=self.name, c=constant),
                'flood': """select CensusBlock as block, Sum(ISNULL(TotalLoss, 0))* {c} as EconLoss from {s}.dbo.flFRGBSEcLossByTotal group by CensusBlock""".format(s=self.name, c=constant),
                'hurricane': """select TRACT as tract, SUM(ISNULL(TotLoss, 0)) * {c} as EconLoss from {s}.dbo.[huSummaryLoss] group by Tract""".format(s=self.name, c=constant),
                'tsunami': """select CensusBlock as block, SUM(ISNULL(TotalLoss, 0)) * {c} as EconLoss from {s}.dbo.tsuvResDelKTotB group by CensusBlock""".format(s=self.name, c=constant)
            }

            df = self.query(sqlDict[self.hazards[0]])
            return StudyRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getTotalEconomicLoss(self):
        """
        Queries the total economic loss summation for a study region from the local Hazus SQL Server database

            Returns:
                totalLoss: integer -- the summation of total economic loss
        """
        totalLoss = self.getEconomicLoss()['EconLoss'].sum()
        return totalLoss

    def getBuildingDamage(self):
        try:
            sqlDict = {
                'earthquake': """SELECT Tract as tract, SUM(ISNULL(PDsNoneBC, 0))
                        As NoDamage, SUM(ISNULL(PDsSlightBC, 0)) AS Affected, SUM(ISNULL(PDsModerateBC, 0))
                        AS Minor, SUM(ISNULL(PDsExtensiveBC, 0)) AS Major,
                        SUM(ISNULL(PDsCompleteBC, 0)) AS Destroyed FROM [{s}].dbo.[eqTractDmg]
                        WHERE DmgMechType = 'STR' group by Tract
                """.format(s=self.name),
                'flood': """SELECT CensusBlock as block, SUM(ISNULL(TotalLoss, 0))
                        AS TotalLoss, SUM(ISNULL(BuildingLoss, 0)) AS BldgLoss,
                        SUM(ISNULL(ContentsLoss, 0)) AS ContLoss
                        FROM [{s}].dbo.[flFRGBSEcLossBySOccup] GROUP BY CensusBlock
                        """.format(s=self.name),
                'hurricane': """SELECT Tract AS tract,
                        SUM(ISNULL(NonDamage, 0)) As NoDamage, SUM(ISNULL(MinDamage, 0)) AS Affected,
                        SUM(ISNULL(ModDamage, 0)) AS Minor, SUM(ISNULL(SevDamage, 0)) AS Major,
                        SUM(ISNULL(ComDamage, 0)) AS Destroyed FROM [{s}].dbo.[huSummaryDamage]
                        WHERE GenBldgOrGenOcc IN('COM', 'AGR', 'GOV', 'EDU', 'REL','RES', 'IND')
                        GROUP BY Tract""".format(s=self.name),
                'tsunami': """select CBFips as block,
                        count(case when BldgLoss/(ValStruct+ValCont) <=0.05 then 1 end) as Affected,
                        count(case when BldgLoss/(ValStruct+ValCont) > 0.05 and BldgLoss/(ValStruct+ValCont) <=0.3 then 1 end) as Minor,
                        count(case when BldgLoss/(ValStruct+ValCont) > 0.3 and BldgLoss/(ValStruct+ValCont) <=0.5 then 1 end) as Major,
                        count(case when BldgLoss/(ValStruct+ValCont) >0.5 then 1 end) as Destroyed
                        from (select NsiID, ValStruct, ValCont  from {s}.dbo.tsHazNsiGbs) haz
                            left join (select NsiID, CBFips from {s}.dbo.tsNsiGbs) gbs
                            on haz.NsiID = gbs.NsiID
                            left join (select NsiID, BldgLoss from {s}.dbo.tsFRNsiGbs) frn
                            on haz.NsiID = frn.NsiID
                            group by CBFips""".format(s=self.name)

            }

            df = self.query(sqlDict[self.hazards[0]])
            return StudyRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getBuildingDamageByOccupancy(self):
        """ Queries the building damage by occupancy type for a study region from the local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of building damage by occupancy type
        """
        try:

            sqlDict = {
                'earthquake': """SELECT Occupancy, SUM(ISNULL(PDsNoneBC, 0))
                        As NoDamage, SUM(ISNULL(PDsSlightBC, 0)) AS Affected, SUM(ISNULL(PDsModerateBC, 0))
                        AS Minor, SUM(ISNULL(PDsExtensiveBC, 0)) AS Major,
                        SUM(ISNULL(PDsCompleteBC, 0)) AS Destroyed FROM {s}.dbo.[eqTractDmg]
                        WHERE DmgMechType = 'STR' GROUP BY Occupancy""".format(s=self.name),
                'flood': """SELECT SOccup AS Occupancy, SUM(ISNULL(TotalLoss, 0))
                        AS TotalLoss, SUM(ISNULL(BuildingLoss, 0)) AS BldgLoss,
                        SUM(ISNULL(ContentsLoss, 0)) AS ContLoss
                        FROM {s}.dbo.[flFRGBSEcLossBySOccup] GROUP BY SOccup""".format(s=self.name),
                'hurricane': """SELECT GenBldgOrGenOcc AS Occupancy,
                        SUM(ISNULL(NonDamage, 0)) As NoDamage, SUM(ISNULL(MinDamage, 0)) AS Affected,
                        SUM(ISNULL(ModDamage, 0)) AS Minor, SUM(ISNULL(SevDamage, 0)) AS Major,
                        SUM(ISNULL(ComDamage, 0)) AS Destroyed FROM {s}.dbo.[huSummaryDamage]
                        WHERE GenBldgOrGenOcc IN('COM', 'AGR', 'GOV', 'EDU', 'REL','RES', 'IND')
                        GROUP BY GenBldgOrGenOcc""".format(s=self.name),
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
                        GROUP BY LEFT({s}.dbo.tsHazNsiGbs.NsiID, 3)""".format(s=self.name)
            }

            df = self.query(sqlDict[self.hazards[0]])
            return StudyRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getBuildingDamageByType(self):
        """ Queries the building damage by structure type for a study region from the local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of building damage by structure type
        """
        try:

            sqlDict = {
                'earthquake': """SELECT eqBldgType AS BldgType,
                        SUM(ISNULL(PDsNoneBC, 0)) As NoDamage, SUM(ISNULL(PDsSlightBC, 0)) AS Affected,
                        SUM(ISNULL(PDsModerateBC, 0)) AS Minor, SUM(ISNULL(PDsExtensiveBC, 0))
                        AS Major, SUM(ISNULL(PDsCompleteBC, 0)) AS Destroyed
                        FROM {s}.dbo.[eqTractDmg] WHERE DmgMechType = 'STR'
                        GROUP BY eqBldgType""".format(s=self.name),
                'flood': """SELECT BldgType, SUM(ISNULL(TotalLoss, 0)) AS TotalLoss,
                        SUM(ISNULL(BuildingLoss, 0)) AS BldgLoss, SUM(ISNULL(ContentsLoss, 0)) AS ContLoss
                        FROM {s}.dbo.[flFRGBSEcLossByGBldgType] GROUP BY BldgType""".format(s=self.name),
                'hurricane': """SELECT GenBldgOrGenOcc AS Occupancy,
                        SUM(ISNULL(NonDamage, 0)) As NoDamage, SUM(ISNULL(MinDamage, 0)) AS Affected,
                        SUM(ISNULL(ModDamage, 0)) AS Minor, SUM(ISNULL(SevDamage, 0)) AS Major,
                        SUM(ISNULL(ComDamage, 0)) AS Destroyed FROM {s}.dbo.[huSummaryDamage]
                        WHERE GenBldgOrGenOcc IN('CONCRETE', 'MASONRY', 'STEEL', 'WOOD', 'MH')
                        GROUP BY GenBldgOrGenOcc""".format(s=self.name),
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
                        GROUP BY eqBldgType, [Description]""".format(s=self.name)
            }

            df = self.query(sqlDict[self.hazards[0]])
            return StudyRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getInjuries(self):
        """ Queries the injuries for a study region from the local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of injuries
        """
        try:

            # NOTE injuries not available for flood model - placeholder below
            # NOTE injuries not available for hurricane model - placeholder below
            sqlDict = {
                'earthquake': """SELECT Tract as tract, SUM(CASE WHEN CasTime = 'N' THEN Level1Injury
                        ELSE 0 END) AS Injury_NightLevel1, SUM(CASE WHEN CasTime = 'N'
                        THEN Level2Injury ELSE 0 END) AS Injury_NightLevel2, SUM(CASE WHEN CasTime = 'N'
                        THEN Level3Injury ELSE 0 END) AS Injury_NightLevel3, SUM(CASE WHEN CasTime = 'N'
                        THEN Level1Injury ELSE 0 END) AS Injury_DayLevel1,  SUM(CASE WHEN CasTime = 'D'
                        THEN Level2Injury ELSE 0 END) AS Injury_DayLevel2, SUM(CASE WHEN CasTime = 'D'
                        THEN Level3Injury ELSE 0 END) AS Injury_DayLevel3 FROM {s}.dbo.[eqTractCasOccup]
                        WHERE CasTime IN ('N', 'D') AND InOutTot = 'Tot' GROUP BY Tract""".format(s=self.name),
                'flood': None,
                'hurricane': None,
                'tsunami': """SELECT
                        cdf.CensusBlock as block,
                        SUM(cdf.InjuryDayTotal) as Injuries_DayFair,
                        SUM(cdg.InjuryDayTotal) As Injuries_DayGood,
                        SUM(cdp.InjuryDayTotal) As Injuries_DayPoor,
                        SUM(cnf.InjuryNightTotal) As Injuries_NightFair,
                        SUM(cng.InjuryNightTotal) As Injuries_NightGood,
                        SUM(cnp.InjuryNightTotal) As Injuries_NightPoor
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
                                group by cdf.CensusBlock""".format(s=self.name)
            }
            if (sqlDict[self.hazards[0]] == None) and self.hazards[0] == 'hurricane':
                df = pd.DataFrame(columns=['tract', 'Injuries'])
            elif (sqlDict[self.hazards[0]] == None) and self.hazards[0] == 'flood':
                df = pd.DataFrame(columns=['block', 'Injuries'])
            else:
                df = self.query(sqlDict[self.hazards[0]])
            return StudyRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getFatalities(self):
        """ Queries the fatalities for a study region from the local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of fatalities
        """
        try:

            # NOTE fatatilies not available for flood model - placeholder below
            # NOTE fatatilies not available for hurricane model - placeholder below
            sqlDict = {
                'earthquake': """SELECT Tract as tract, SUM(CASE WHEN CasTime = 'N'
                        THEN Level4Injury ELSE 0 End) AS Fatalities_Night, SUM(CASE WHEN CasTime = 'D'
                        THEN Level4Injury ELSE 0 End) AS Fatalities_Day FROM {s}.dbo.[eqTractCasOccup]
                        WHERE CasTime IN ('N', 'D') AND InOutTot = 'Tot' GROUP BY Tract""".format(s=self.name),
                'flood': None,
                'hurricane': None,
                'tsunami': """SELECT
                        cdf.CensusBlock as block,
                        SUM(cdf.FatalityDayTotal) As Fatalities_DayFair,
                        SUM(cdg.FatalityDayTotal) As Fatalities_DayGood,
                        SUM(cdp.FatalityDayTotal) As Fatalities_DayPoor,
                        SUM(cnf.FatalityNightTotal) As Fatalities_NightFair,
                        SUM(cng.FatalityNightTotal) As Fatalities_NightGood,
                        SUM(cnp.FatalityNightTotal) As Fatalities_NightPoor
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
                                group by cdf.CensusBlock""".format(s=self.name)
            }

            if (sqlDict[self.hazards[0]] == None) and self.hazards[0] == 'hurricane':
                df = pd.DataFrame(columns=['tract', 'Fatalities'])
            elif (sqlDict[self.hazards[0]] == None) and self.hazards[0] == 'flood':
                df = pd.DataFrame(columns=['block', 'Fatalities'])
            else:
                df = self.query(sqlDict[self.hazards[0]])
            return StudyRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getDisplacedHouseholds(self):
        """ Queries the displaced households for a study region from the local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of displaced households
        """
        try:

            # TODO check to see if flood is displaced households or population -- database says pop
            # NOTE displaced households not available in tsunami model - placeholder below
            sqlDict = {
                'earthquake': """select Tract as tract, SUM(DisplacedHouseholds) as DisplacedHouseholds from {s}.dbo.eqTract group by Tract""".format(s=self.name),
                'flood': """select CensusBlock as block, SUM(DisplacedPop) as DisplacedHouseholds from {s}.dbo.flFRShelter group by CensusBlock""".format(s=self.name),
                'hurricane': """select TRACT as tract, SUM(DISPLACEDHOUSEHOLDS) as DisplacedHouseholds from {s}.dbo.huShelterResultsT group by Tract""".format(s=self.name),
                'tsunami': None
            }

            if (sqlDict[self.hazards[0]] == None) and self.hazards[0] == 'tsunami':
                df = pd.DataFrame(columns=['block', 'DisplacedHouseholds'])
            else:
                df = self.query(sqlDict[self.hazards[0]])
            return StudyRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getShelterNeeds(self):
        """ Queries the short term shelter needs for a study region from the local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of short term shelter needs
        """
        try:

            # NOTE shelter needs aren't available for the tsunami model - placeholder below
            sqlDict = {
                'earthquake': """select Tract as tract, SUM(ShortTermShelter) as ShelterNeeds from {s}.dbo.eqTract group by Tract""".format(s=self.name),
                'flood': """select CensusBlock as block, SUM(ShortTermNeeds) as ShelterNeeds from {s}.dbo.flFRShelter group by CensusBlock""".format(s=self.name),
                'hurricane': """select TRACT as tract, SUM(SHORTTERMSHELTERNEEDS) as ShelterNeeds from {s}.dbo.huShelterResultsT group by Tract
                        """.format(s=self.name),
                'tsunami': None
            }
            if (sqlDict[self.hazards[0]] == None) and self.hazards[0] == 'tsunami':
                df = pd.DataFrame(columns=['block', 'ShelterNeeds'])
            else:
                df = self.query(sqlDict[self.hazards[0]])
            return StudyRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getDebris(self):
        """ Queries the debris for a study region from the local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of debris
        """
        try:
            constant = 1000
            # NOTE debris not available for tsunami model - placeholder below
            sqlDict = {
                'earthquake': """select Tract as tract, SUM(DebrisW) * {c} as DebrisBW, SUM(DebrisS) * {c} as DebrisCS, SUM(DebrisTotal) * {c} as DebrisTotal from {s}.dbo.eqTract group by Tract""".format(s=self.name, c=constant),
                'flood': """select CensusBlock as block, SUM(FinishTons) * {c} as DebrisTotal from {s}.dbo.flFRDebris group by CensusBlock""".format(s=self.name, c=constant),
                'hurricane': """select Tract as tract, SUM(BRICKANDWOOD) * {c} as DebrisBW, SUM(CONCRETEANDSTEEL) * {c} as DebrisCS, SUM(Tree) as DebrisTree, SUM(BRICKANDWOOD + CONCRETEANDSTEEL + Tree) as DebrisTotal from {s}.dbo.huDebrisResultsT group by Tract""".format(s=self.name, c=constant),
                'tsunami': """select CensusBlock as block, SUM(FinishTons) * {c} as DebrisTotal from {s}.dbo.flFRDebris group by CensusBlock""".format(s=self.name, c=constant)
            }

            df = self.query(sqlDict[self.hazards[0]])
            return StudyRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getHazardsAnalyzed(self, returnType='list'):
        # TODO fix this when multiple hazards are available
        """ Queries the local Hazus SQL Server database and returns all hazards analyzed

            Key Argument:
                returnType: string -- choices: 'list', 'dict'
            Returns:
                df: pandas dataframe -- a dataframe of the hazards analyzed
        """
        try:
            sql = "select * from [syHazus].[dbo].[syStudyRegion] where [RegionName] = '" + \
                self.name + "'"
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

    def getHazardDictionary(self):
        """ Queries the local Hazus SQL Server database and returns a geodataframe of the hazard

            Returns:
                hazardDict: dictionary<geopandas geodataframe> -- a dictionary containing geodataframes of the hazard(s)
        """
        try:
            hazard = self.hazards[0]
            hazardDict = {}
            if hazard == 'earthquake':
                path = 'C:/HazusData/Regions/'+self.name+'/shape/pga.shp'
                gdf = gpd.read_file(path)
                hazardDict['Peak Ground Acceleration (g)'] = gdf
            if hazard == 'flood':
                sql = """SELECT [StudyCaseName] FROM {s}.[dbo].[flStudyCase]""".format(
                    s=self.name)
                scenarios = self.query(sql)
                for scenario in scenarios['StudyCaseName']:
                    paths = {
                        'Water Depth (ft) - Deterministic Riverine': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Riverine/Depth/mix0/w001001.adf',
                        'Water Depth (ft) - Deterministic Coastal': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Coastal/Depth/mix0/w001001.adf',
                        'Water Depth (ft) - Probabilistic Riverine 5-year': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Riverine/Depth/rpd5/w001001.adf',
                        'Water Depth (ft) - Probabilistic Riverine 10-year': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Riverine/Depth/rpd10/w001001.adf',
                        'Water Depth (ft) - Probabilistic Riverine 25-year': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Riverine/Depth/rpd25/w001001.adf',
                        'Water Depth (ft) - Probabilistic Riverine 50-year': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Riverine/Depth/rpd50/w001001.adf',
                        'Water Depth (ft) - Probabilistic Riverine 100-year': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Riverine/Depth/rpd100/w001001.adf',
                        'Water Depth (ft) - Probabilistic Riverine 500-year': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Riverine/Depth/rpd500/w001001.adf',
                        'Water Depth (ft) - Probabilistic Coastal 5-year': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Coastal/Depth/rpd5/w001001.adf',
                        'Water Depth (ft) - Probabilistic Coastal 10-year': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Coastal/Depth/rpd10/w001001.adf',
                        'Water Depth (ft) - Probabilistic Coastal 25-year': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Coastal/Depth/rpd25/w001001.adf',
                        'Water Depth (ft) - Probabilistic Coastal 50-year': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Coastal/Depth/rpd50/w001001.adf',
                        'Water Depth (ft) - Probabilistic Coastal 100-year': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Coastal/Depth/rpd100/w001001.adf',
                        'Water Depth (ft) - Probabilistic Coastal 500-year': 'C:/HazusData/Regions/'+self.name+'/' +
                        scenario+'/Coastal/Depth/rpd500/w001001.adf',
                    }
                    for key in paths.keys():
                        try:
                            raster = rio.open(paths[key])
                            affine = raster.meta.get('transform')
                            crs = raster.meta.get('crs')
                            band = raster.read(1)
                            # band = np.where(band < 0, 0, band)

                            geoms = []
                            for geometry, value in features.shapes(band, transform=affine):
                                try:
                                    if value >= 1:
                                        result = {'properties': {
                                            'PARAMVALUE': value}, 'geometry': geometry}
                                    geoms.append(result)
                                except:
                                    pass
                            gdf = gpd.GeoDataFrame.from_features(geoms)
                            hazardDict[key] = gdf
                        except:
                            pass
            if hazard == 'hurricane':
                try:
                    queries = {
                        'Wind Speeds (mph) - Deterministic': 'SELECT Tract as tract, PeakGust as PARAMVALUE FROM {s}.[dbo].[hv_huDeterminsticWindSpeedResults]'.format(
                            s=self.name),
                        'Wind Speeds (mph) - Probabilistic 10-year': 'SELECT Tract as tract, f10yr as PARAMVALUE FROM {s}.[dbo].[huHazardMapWindSpeed] where f10yr > 0'.format(
                            s=self.name),
                        'Wind Speeds (mph) - Probabilistic 20-year': 'SELECT Tract as tract, f20yr as PARAMVALUE FROM {s}.[dbo].[huHazardMapWindSpeed] where f20yr > 0'.format(
                            s=self.name),
                        'Wind Speeds (mph) - Probabilistic 50-year': 'SELECT Tract as tract, f50yr as PARAMVALUE FROM {s}.[dbo].[huHazardMapWindSpeed] where f50yr > 0'.format(
                            s=self.name),
                        'Wind Speeds (mph) - Wind Speeds 100-year': 'SELECT Tract as tract, f100yr as PARAMVALUE FROM {s}.[dbo].[huHazardMapWindSpeed] where f100yr > 0'.format(
                            s=self.name),
                        'Wind Speeds (mph) - Probabilistic 200-year': 'SELECT Tract as tract, f200yr as PARAMVALUE FROM {s}.[dbo].[huHazardMapWindSpeed] where f200yr > 0'.format(
                            s=self.name),
                        'Wind Speeds (mph) - Probabilistic 500-year': 'SELECT Tract as tract, f500yr as PARAMVALUE FROM {s}.[dbo].[huHazardMapWindSpeed] where f500yr > 0'.format(
                            s=self.name),
                        'Wind Speeds (mph) - Probabilistic 1000-year': 'SELECT Tract as tract, f1000yr as PARAMVALUE FROM {s}.[dbo].[huHazardMapWindSpeed] where f1000yr > 0'.format(
                            s=self.name)
                    }
                    for key in queries.keys():
                        try:
                            df = self.query(queries[key])
                            if len(df) > 0:
                                sdf = StudyRegionDataFrame(self, df)
                                sdf = sdf.addGeometry()
                                sdf['geometry'] = sdf['geometry'].apply(loads)
                                gdf = gpd.GeoDataFrame(
                                    sdf, geometry='geometry')
                                hazardDict[key] = gdf
                        except:
                            pass
                except:
                    pass

            if hazard == 'tsunami':
                raster = rio.open(
                    r'C:\HazusData\Regions\{s}\maxdg_dft\w001001.adf'.format(s=self.name))
                affine = raster.meta.get('transform')
                band = raster.read(1)
                # band = np.where(band < 0, 0, band)

                geoms = []
                for geometry, value in features.shapes(band, transform=affine):
                    try:
                        if value >= 1:
                            result = {'properties': {
                                'PARAMVALUE': value}, 'geometry': geometry}
                        geoms.append(result)
                    except:
                        pass
                gdf = gpd.GeoDataFrame.from_features(geoms)
                gdf.PARAMVALUE[gdf.PARAMVALUE > 60] = 0
                hazardDict['Water Depth (ft)'] = gdf
            return hazardDict
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getEssentialFacilities(self):
        """ Queries the call essential facilities for a study region in local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of the essential facilities and damages
        """
        try:
            essentialFacilities = ['AirportFlty', 'BusFlty', 'CareFlty', 'CommunicationFlty',
                                   'Dams', 'ElectricPowerFlty', 'EmergencyCtr', 'FerryFlty', 'FireStation',
                                   'HighwayBridge', 'HighwaySegment', 'HighwayTunnel', 'Levees', 'LightRailBridge',
                                   'LightRailFlty', 'LightRailSegment', 'LightRailTunnel', 'Military',
                                   'NaturalGasFlty', 'NaturalGasPl', 'NuclearFlty', 'OilFlty', 'OilPl',
                                   'PoliceStation', 'PortFlty', 'PotableWaterFlty', 'RailFlty',
                                   'RailwayBridge', 'RailwaySegment', 'RailwayTunnel', 'Runway', 'School',
                                   'WasteWaterFlty', 'WasteWaterPl']

            prefixDict = {
                'earthquake': 'eq',
                'hurricane': 'huResults',
                'flood': 'flFR',
                'tsunami': 'ts'
            }
            prefix = prefixDict[self.hazards[0]]

            essentialFacilityDataFrames = {}
            for facility in essentialFacilities:
                try:
                    # get all column names for study region table
                    sql = """SELECT COLUMN_NAME as "fieldName" FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{p}{f}'""".format(
                        f=facility, p=prefix)
                    df = self.query(sql)
                    if len(df) > 0:
                        srcolumns = df['fieldName'].tolist()
                        # remove confounding columns
                        if 'StudyCaseId' in srcolumns:
                            srcolumns.remove('StudyCaseId')
                        if 'ReturnPeriodId' in srcolumns:
                            srcolumns.remove('ReturnPeriodId')

                        # get Id column name
                        idColumnList = [x for x in srcolumns if facility in x]
                        if len(idColumnList) == 0:
                            idColumnList = [
                                x for x in srcolumns if x.endswith('Id')]
                        idColumn = idColumnList[0]

                        # build query fields for study region table
                        tempColumns = [x.replace(x, '['+x+']')
                                       for x in srcolumns]
                        tempColumns.insert(0, "'"+facility+"'" +
                                           ' as "FacilityType"')
                        tempColumns.insert(0, '['+idColumn+'] as FacilityId')
                        studyRegionColumns = ', '.join(tempColumns)

                        # get all column names for hz table
                        sql = """SELECT COLUMN_NAME as "fieldName" FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'hz{f}'""".format(
                            f=facility, p=prefix)
                        df = self.query(sql)
                        hzcolumns = df['fieldName'].tolist()

                        # build query fields for hz table
                        containFields = ['Name', 'City',
                                         'County', 'State', 'Fips', 'Shape']
                        # limit fields to containFields
                        hzcolumns = [x for x in hzcolumns if any(
                            f in x for f in containFields)]
                        tempColumns = [x.replace(x, '['+x+']')
                                       for x in hzcolumns]
                        tempColumns = [x.replace('[Shape]', 'Shape.STAsText() as geometry')
                                       for x in tempColumns]
                        tempColumns = [x.replace('[Statea]', '[Statea] as State')
                                       for x in tempColumns]
                        tempColumns.insert(0, '['+idColumn+'] as FacilityId')
                        hazusColumns = ', '.join(tempColumns)

                        # build queryset columns
                        # replace hzcolumns
                        hzcolumns = [x.replace('Statea', 'State')
                                     for x in hzcolumns]
                        hzcolumns = [x.replace('Shape', 'geometry')
                                     for x in hzcolumns]
                        # replace srcolumns
                        srcolumns = [x.replace(idColumn, 'FacilityId')
                                     for x in srcolumns]
                        srcolumns.insert(0, 'FacilityType')
                        hzcolumnsFinal = ', '.join(
                            ['hz.' + x for x in hzcolumns])
                        srcolumnsFinal = ', '.join(
                            ['sr.' + x for x in srcolumns])
                        querysetColumns = ', '.join(
                            [srcolumnsFinal, hzcolumnsFinal])

                        try:
                            # build dynamic sql query
                            sql = """
                                    SELECT
                                        {qc}
                                        FROM
                                        (SELECT
                                            {src}
                                            from [{s}].[dbo].[{p}{f}]
                                            where EconLoss > 0) sr
                                        left join
                                        (SELECT
                                            {hzc}
                                            from [{s}].[dbo].[hz{f}]) hz
                                        on hz.FacilityID = sr.FacilityID
                                    """.format(i=idColumn, s=self.name, f=facility, p=prefix, qc=querysetColumns, src=studyRegionColumns, hzc=hazusColumns)

                            # get queryset from database
                            df = self.query(sql)
                        except:
                            # build dynamic sql query without the where clause
                            sql = """
                                    SELECT
                                        {qc}
                                        FROM
                                        (SELECT
                                            {src}
                                            from [{s}].[dbo].[{p}{f}]) sr
                                        left join
                                        (SELECT
                                            {hzc}
                                            from [{s}].[dbo].[hz{f}]) hz
                                        on hz.FacilityID = sr.FacilityID
                                    """.format(i=idColumn, s=self.name, f=facility, p=prefix, qc=querysetColumns, src=studyRegionColumns, hzc=hazusColumns)

                            # get queryset from database
                            df = self.query(sql)
                        # check if the queryset contains data
                        if len(df) > 1:
                            # convert all booleans to string
                            mask = df.applymap(type) != bool
                            replaceDict = {True: 'TRUE', False: 'FALSE'}
                            df = df.where(mask, df.replace(replaceDict))
                            # add to dictionary
                            essentialFacilityDataFrames[facility] = df
                    else:
                        pass
                except:
                    print("Unexpected error:", sys.exc_info()[0])
                    pass
            # if essentialFacilityDataFrames contains data, concatenate into a dataframe
            if len(essentialFacilityDataFrames) > 0:
                essentialFacilityDf = pd.concat(
                    [x.fillna('null') for x in essentialFacilityDataFrames.values()], sort=False).fillna('null')
                return StudyRegionDataFrame(self, essentialFacilityDf)
            else:
                print("No essential facility loss information for " +
                      self.name)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
            """
            import hazpy
            sr = hazpy.legacy.StudyRegion('ts_test')
            ef = sr.getEssentialFacilities()
            ef = sr.getResults()
            print(ef)
            print(ef.head())
            ef.toCSV('C:/Users/jrainesi/Downloads/apples.csv')
            ef.toShapefile('C:/Users/jrainesi/Downloads/apples.shp')
            ef.toGeoJSON('C:/Users/jrainesi/Downloads/apples.geojson')
            """

    def getDemographics(self):
        """Summarizes demographics at the lowest level of geography

            Returns:
                df: pandas dataframe -- a dataframe of the summarized demographics
        """
        try:

            sqlDict = {
                'earthquake': """select Tract as tract, Population, Households FROM {s}.dbo.[hzDemographicsT]""".format(s=self.name),
                'flood': """select CensusBlock as block, Population, Households FROM {s}.dbo.[hzDemographicsB]""".format(s=self.name),
                'hurricane': """select Tract as tract, Population, Households FROM {s}.dbo.[hzDemographicsT]""".format(s=self.name),
                'tsunami': """select CensusBlock as block, Population, Households FROM {s}.dbo.[hzDemographicsB]""".format(s=self.name)
            }

            df = self.query(sqlDict[self.hazards[0]])
            return StudyRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getResults(self):
        """ Summarizes results at the lowest level of geography

            Returns:
                df: pandas dataframe -- a dataframe of the summarized results
        """
        try:
            economicLoss = self.getEconomicLoss()
            buildingDamage = self.getBuildingDamage()
            fatalities = self.getFatalities()
            injuries = self.getInjuries()
            shelterNeeds = self.getShelterNeeds()
            displacedHouseholds = self.getDisplacedHouseholds()
            debris = self.getDebris()
            demographics = self.getDemographics()

            dataFrameList = [economicLoss, buildingDamage, fatalities,
                             injuries, shelterNeeds, displacedHouseholds, debris, demographics]

            if 'block' in economicLoss.columns:
                dfMerged = reduce(lambda left, right: pd.merge(
                    left, right, on=['block'], how='outer'), dataFrameList)
            elif 'tract' in economicLoss.columns:
                dfMerged = reduce(lambda left, right: pd.merge(
                    left, right, on=['tract'], how='outer'), dataFrameList)
            elif 'county' in economicLoss.columns:
                dfMerged = reduce(lambda left, right: pd.merge(
                    left, right, on=['county'], how='outer'), dataFrameList)

            df = dfMerged[dfMerged['EconLoss'].notnull()]
            # Find the columns where each value is null
            empty_cols = [col for col in df.columns if df[col].isnull().all()]
            # Drop these columns from the dataframe
            df.drop(empty_cols, axis=1, inplace=True)

            return StudyRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getCounties(self):
        """Creates a dataframe of the county name and geometry for all counties in the study region

            Returns:
                gdf: geopandas geodataframe -- a geodataframe of the counties
        """
        try:

            sql = """SELECT 
                        CountyName as "name",
                        NumAggrTracts as "size",
                        Shape.STAsText() as "geometry"
                        FROM [{s}].[dbo].[hzCounty]
                """.format(s=self.name)

            df = self.query(sql)
            df['geometry'] = df['geometry'].apply(loads)
            gdf = gpd.GeoDataFrame(df, geometry='geometry')
            return gdf
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getTravelTimeToSafety(self):
        """Creates a geodataframe of the travel time to safety

            Returns:
                gdf: geopandas geodataframe -- a geodataframe of the counties
        """
        if self.hazards[0] == 'tsunami':
            try:

                sql = """SELECT
                    tiger.CensusBlock,
                    tiger.Tract, tiger.Shape.STAsText() AS geometry,
                    ISNULL(travel.Trav_SafeUnder65, 0) as travelTimeUnder65yo,
                    ISNULL(travel.Trav_SafeOver65, 0) as travelTimeOver65yo
                        FROM {s}.dbo.[hzCensusBlock_TIGER] as tiger
                            FULL JOIN {s}.dbo.tsTravelTime as travel
                                ON tiger.CensusBlock = travel.CensusBlock""".format(s=self.name)

                df = self.query(sql)
                df['geometry'] = df['geometry'].apply(loads)
                gdf = gpd.GeoDataFrame(df, geometry='geometry')
                return gdf
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise
        else:
            print("This method is only available for tsunami study regions")


"""
def essentialFacilities
def transportation
def agriculture
def vehicles?
def GBS?
"""
