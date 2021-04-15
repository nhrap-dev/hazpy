import os
import pandas as pd
import geopandas as gpd
import pyodbc as py
from shapely.wkt import loads
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
import urllib
import zipfile
import shutil
from pathlib import Path
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

from .hazuspackageregiondataframe import HazusPackageRegionDataFrame #might be able to switch back to RegionDataFrame, since adding name property
from .report import Report



class HazusPackageRegion():
    """ Creates an HazusPackageRegion object from an Hazus Package Region (hpr) file and has functions to batch export.
        1 Must run function to restore database before exporting
        2 Must run exports functions
        3 Must run cleanup functions
        Set the hazard,scenario,returnperiod to export?

        Keyword Arguments:
            hprFilePath: str -- the path of the .hpr file

        Notes: Should create a directory for each Hazard/Studycase|Scenario/ScenarioType/ReturnPeriod for exported products.
                Also should create a main spreadsheet metadata logging what it created and any issues/errors.

    """
    def __init__(self, hprFilePath, outputDir):
        """Blank properties are filled in via functions.
        """
        self.hprFilePath = Path(hprFilePath)
        self.outputDir = Path.joinpath(Path(outputDir), self.hprFilePath.stem)
        self.tempDir = Path.joinpath(Path(outputDir), self.hprFilePath.stem + '_temp')
        
        self.name = '' #bkifilename/database #also used in HazusPackageRegionDataFrame
        self.hazard = ''
        self.scenario = ''
        self.returnPeriod = ''
        self.hazusPackageRegion = self.hprFilePath.stem #for HazusPackageRegionDataFrame

        self.hprComment = self.getHPRComment(self.hprFilePath)
        self.HazusVersion = self.getHPRHazusVersion(self.hprComment)
        self.Hazards = self.getHPRHazards(self.hprComment)
        self.bkFilePath = ''
        self.dbName = ''
        self.LogicalNames = []
        self.LogicalName_data = ''
        self.LogicalName_log = '' #tuple ()
        
##        self.floodStudyCases = []
##        self.floodStudyCaseRiverineReturnPeriods = []
##        self.floodStudyCaseCoastalPeriods = []
##        self.floodStudyCaseRiverineCoastalPeriods = []

    #GET .HPR FILE INFO
    def getHPRComment(self, hprPath):
        """Read an .hpr/zipfiles comments and assign to class property.

        Keyword Arguments:
            hprPath: str -- a string of the full directory path and hpr filename.

        Returns:
            zComment: list 
    
        Notes: 
        """
        z = zipfile.ZipFile(hprPath)
        zComment = z.comment.decode('UTF-8').split('|')
        return zComment

    def getHPRHazusVersion(self, hprComment):
        """
        Keyword Arguments:
            hprComment: list 

        Returns:
            hprHazusVersion: string
            
        Notes:  Export is only going to support HPR as far back as Hazus 2.0.
           Version|RegionName|.bk|Earthquake|Flood|Hurricane
           '31ed16|121212|NorCal-BayArea_SanAndreasM7-8|NorCal-BayArea_SanAndreasM7-8.bk|1|0|0'

           Version|RegionName|.bk|Earthquake|Flood|Hurricane|Tsunami
           '31ed16|202020|FIMJacksonMO|FIMJacksonMO.bk|0|1|0|0'
           
           The first pipe is unknown what it is.
           EQ added 1997
           FL added 2003
           HU 2004
           TS 2017 added in Hazus 4.0
        """
        versionLookupDict = { '060606':'Hazus MR1'
                             ,'070707':'Hazus MR2'
                             ,'080808':'Hazus MR3'
                             ,'090909':'Hazus MR4'
                             ,'101010':'Hazus MR5'
                             ,'111111':'Hazus 2.0'
                             ,'121212':'Hazus 2.1'
                             ,'131313':'Hazus 3.0'
                             ,'141414':'Hazus 3.1'
                             ,'151515':'Hazus 4.0'
                             ,'161616':'Hazus 4.1'
                             ,'171717':'Hazus 4.2'
                             ,'181818':'Hazus 4.2.1'
                             ,'191919':'Hazus 4.2.2'
                             ,'202020':'Hazus 4.2.3'
                             ,'212121':'Hazus 5.0'}
        commentVersion = hprComment[1]
        if commentVersion in versionLookupDict:
            hprHazusVersion = versionLookupDict[commentVersion]
            return hprHazusVersion
        else:
            print(f'{zVersion} not in Hazus version list.')

    def getHPRHazards(self, hprComment, returnType='list'):
        """Compare comments value to known Hazus version value to get common Hazus version.

        Keyword Arguments:
            hprComment: list 

        Returns:
            hazardsList: list
            hazardsDict: dictionary

        Notes:
            ['earthquake','flood','hurricane','tsunami']
            {'earthquake':0|1,'flood':0|1,'hurricane':0|1,'tsunami':0|1}
        """
        #handle hpr after Hazus 4.0
        if len(hprComment) == 8:
            zRegionName = hprComment[2]
            hazardsDict = {
                'earthquake': int(hprComment[4]),
                'flood': int(hprComment[5]),
                'hurricane': int(hprComment[6]),
                'tsunami': int(hprComment[7])}
        #handle hpr before Hazus 4.0
        elif len(hprComment) == 7:
            zRegionName = hprComment[2]
            hazardsDict = {
                'earthquake': int(hprComment[4]),
                'flood': int(hprComment[5]),
                'hurricane': int(hprComment[6]),
                'tsunami': 0}
        if returnType == 'dict':
            return hazardsDict
        if returnType == 'list':
            hazardsList = list(
                filter(lambda x: hazardsDict[x], hazardsDict))
            return hazardsList



    #RESTORE .HPR TO HAZUS SQL SERVER
    def unzipHPR(self, hprPath, tempDir):
        """Unzip to temp folder.


        """
        print(f'Unzipping {hprPath} to {tempDir}...')
        with zipfile.ZipFile(hprPath, 'r') as zip_ref:
            zip_ref.extractall(tempDir)
        print('...done')
        print()

    def getBKFilePath(self, fileDir):
        """

        Note: there should be only one bkfile.
        """
        fileExt = r'*.bk'
        bkList = list(Path(fileDir).glob(fileExt))
        print(f'Available bk files in {fileDir}: {bkList}')
        if len(bkList) > 1:
            print(f'Too many .bk files ({len(bkList)}), choosing the first one {str(bkList[0])}')
            print()
            bkFilePath = Path.joinpath(fileDir, str(bkList[0]))
            self.bkFilePath = bkFilePath
            self.dbName = bkFilePath.stem
            self.name = 'bk_' + self.dbName
        elif len(bkList) == 1:
            print()
            bkFilePath = Path.joinpath(fileDir, str(bkList[0]))
            self.bkFilePath = bkFilePath
            self.dbName = bkFilePath.stem
            self.name = 'bk_' + self.dbName
        else:
            print(f'no bkfile in {fileDir}')
        
    def getFileListHeadersFromDBFile(self, bkFilePath, cursor):
        """
        """
        #Create temp table to hold .bk info...
        print(f'Creating and populating temporary table to hold {bkFilePath} FileListHeaders info...')
        cursor.execute("""CREATE TABLE #FileListHeaders (     
             LogicalName    nvarchar(128)
            ,PhysicalName   nvarchar(260)
            ,[Type] char(1)
            ,FileGroupName  nvarchar(128) NULL
            ,Size   numeric(20,0)
            ,MaxSize    numeric(20,0)
            ,FileID bigint
            ,CreateLSN  numeric(25,0)
            ,DropLSN    numeric(25,0) NULL
            ,UniqueID   uniqueidentifier
            ,ReadOnlyLSN    numeric(25,0) NULL
            ,ReadWriteLSN   numeric(25,0) NULL
            ,BackupSizeInBytes  bigint
            ,SourceBlockSize    int
            ,FileGroupID    int
            ,LogGroupGUID   uniqueidentifier NULL
            ,DifferentialBaseLSN    numeric(25,0) NULL
            ,DifferentialBaseGUID   uniqueidentifier NULL
            ,IsReadOnly bit
            ,IsPresent  bit
            )
            IF cast(cast(SERVERPROPERTY('ProductVersion') as char(4)) as float) > 9
            BEGIN
                ALTER TABLE #FileListHeaders ADD TDEThumbprint  varbinary(32) NULL
            END
            IF cast(cast(SERVERPROPERTY('ProductVersion') as char(2)) as float) > 12
            BEGIN
                ALTER TABLE #FileListHeaders ADD SnapshotURL    nvarchar(360) NULL
            END""")
        cursor.execute(f"INSERT INTO #FileListHeaders EXEC ('RESTORE FILELISTONLY FROM DISK = N''{bkFilePath}''')")
        #Get .bk mdf and log names...
        LogicalName_data = cursor.execute("SELECT LogicalName FROM #FileListHeaders WHERE Type = 'D'").fetchval()
        LogicalName_log = cursor.execute("SELECT LogicalName FROM #FileListHeaders WHERE Type = 'L'").fetchval()
        print(LogicalName_data) 
        print(LogicalName_log)
        cursor.execute("DROP TABLE #FileListHeaders")
        print('...done')
        print()
        return(LogicalName_data, LogicalName_log)

    def createConnection(self, orm='pyodbc'):
        """ Creates a connection object to the local Hazus SQL Server database

            Key Argument:
                orm: string - - type of connection to return (choices: 'pyodbc', 'sqlalchemy')
            Returns:
                conn: pyodbc connection
        """
        try:
            # list all Windows SQL Server drivers
            drivers = [
                '{ODBC Driver 17 for SQL Server}',
                '{ODBC Driver 13.1 for SQL Server}',    
                '{ODBC Driver 13 for SQL Server}',
                '{ODBC Driver 11 for SQL Server} ',
                '{SQL Server Native Client 11.0}',
                '{SQL Server Native Client 10.0}',
                '{SQL Native Client}',
                '{SQL Server}'
            ]
            computer_name = os.environ['COMPUTERNAME']
            if orm == 'pyodbc':
                # create connection with the latest driver
                for driver in drivers:
                    try:
                        conn = py.connect('Driver={d};SERVER={cn}\HAZUSPLUSSRVR; UID=SA;PWD=Gohazusplus_02'.format(
                            d=driver, cn=computer_name))
                        break
                    except:
                        continue
            # TODO add sqlalchemy connection
            # if orm == 'sqlalchemy':
            #     conn = create_engine('mssql+pyodbc://SA:Gohazusplus_02@HAZUSPLUSSRVR')
            # self.conn = conn
            return conn
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def restoreSQLServerBKFile(self, dbName, dirPath, bkFilePath, LogicalName_data, LogicalName_log, cursor):
        """
        Notes: Creates mdf and log files. Runs asynchronously. Database is not available via Hazus Study Regions.
        """
        self.conn = self.createConnection()
        print(f'Restoring database: {dbName} ...')
        mdfPath = Path.joinpath(dirPath, f'{LogicalName_data}.mdf') 
        logPath = Path.joinpath(dirPath, f'{LogicalName_log}.mdf')
        cursor.execute(f"RESTORE DATABASE [bk_{dbName}] FROM DISK='{bkFilePath}' WITH MOVE '{LogicalName_data}' to '{mdfPath}', MOVE '{LogicalName_log}' to '{logPath}'")
        while cursor.nextset():
            pass
        print('...done')
        print()

    def restoreHPR(self):
        """Use several base functions together to effectively attach an hpr file to sql server for access by export functions.


        """
        #UnzipHPR...
        self.unzipHPR(self.hprFilePath, self.tempDir)
        #Find .bk files in unzipped folder...
        self.getBKFilePath(self.tempDir)
        #Connect to SQL Server Hazus...
        self.conn = self.createConnection()
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        #Get .bk file FileListHeaders info...
        self.LogicalNames = self.getFileListHeadersFromDBFile(self.bkFilePath, self.cursor)
        self.LogicalName_data = self.LogicalNames[0]
        self.LogicalName_log = self.LogicalNames[1]
        #Restore the database using the FileListHeaders info...
        self.restoreSQLServerBKFile(self.dbName, self.tempDir, self.bkFilePath, self.LogicalName_data, self.LogicalName_log, self.cursor)



    #CLEANUP
    def detachDB(self):
        """
        """
        print(f'Detaching {self.dbName}...')
        self.cursor.execute(f"USE [master] ALTER DATABASE [{self.dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE") #may help with locks
        ##self.cursor.execute(f"USE [master] EXEC master.dbo.sp_detach_db @dbname = N'bk_{self.dbName}'")
        while self.cursor.nextset():
            pass
        print('...done')
        print()
        
    def deleteDIR(self):
        """
        """
        print(f'Deleting temp folder:{self.tempDir}...')
        try:
            shutil.rmtree(self.tempDir)
        except OSError as e:
            print ("Error: %s - %s." % (e.filename, e.strerror))
        print('...done')
        print()

        
        
    #GET HAZARD, SCENARIO, RETURN PERIOD DATA
    def query(self, sql):
        """Performs a SQL query on the Hazus SQL Server database

            Keyword Arguments:
                sql: str -- a T-SQL query

            Returns:
                df: pandas dataframe
        """
        try:
            df = pd.read_sql(sql, self.conn)
            return HazusPackageRegionDataFrame(self, df)
        except:
            # NOTE: uncomment error print only for debugging
            # print("Unexpected error:", sys.exc_info()[0])
            raise
        
    def getReturnPeriods(self, hazard, scenario):
        """
        """
        try:
            if hazard == 'earthquake':
                sql = f"SELECT [ReturnPeriod] as returnPeriod FROM [bk_{self.dbName}].[dbo].[RgnExpeqScenario]"
            if hazard == 'hurricane':
                sql = f"SELECT DISTINCT [Return_Period] as returnPeriod FROM [bk_{self.dbName}].[dbo].[hv_huQsrEconLoss] WHERE huScenarioName = '{scenario}'"
            if hazard == 'flood':  # TODO test if this works for UDF
                sql = f"""SELECT DISTINCT [ReturnPeriodID] as returnPeriod FROM [bk_{self.dbName}].[dbo].[flFRGBSEcLossByTotal]
                        WHERE StudyCaseId = (SELECT StudyCaseID FROM [bk_{self.dbName}].[dbo].[flStudyCase] WHERE StudyCaseName = '{scenario}')"""
            if hazard == 'tsunami':  # selecting 0 due to no return period existing in database
                sql = f"SELECT '0' as returnPeriod FROM [bk_{self.dbName}].[dbo].[tsScenario]"

            queryset = self.query(sql)
            returnPeriods = list(queryset['returnPeriod'])
            # assign as 0 if no return periods exists
            if len(returnPeriods) == 0:
                returnPeriods.append('0')
            return returnPeriods
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
        
    def getScenarios(self, hazard):
        """
        """
        try:
            if hazard == 'earthquake':
                sql = f"SELECT [eqScenarioname] as scenarios FROM [bk_{self.dbName}].[dbo].[RgnExpeqScenario]"
            if hazard == 'flood':
                sql = f"SELECT [StudyCaseName] as scenarios FROM [bk_{self.dbName}].[dbo].[flStudyCase]"
            if hazard == 'hurricane':
                sql = f"SELECT distinct(huScenarioName) as scenarios FROM [bk_{self.dbName}].dbo.[huSummaryLoss]"
            if hazard == 'tsunami':
                sql = f"SELECT [ScenarioName] as scenarios FROM [bk_{self.dbName}].[dbo].[tsScenario]"
            queryset = self.query(sql)
            scenarios = list(queryset['scenarios'])
            return scenarios
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
            
        
    def getHazardsScenariosReturnPeriods(self):
        """Create a dictionary using the template in the notes so that it can be programmaticaly read to batch export.

        Arguments:
            self.hazard list assumes self.hazard is a list of strings

        Notes:
            ReturnPeriods may have extra spaces.
        
             [{'Hazard':'flood',
               'Scenarios':[
                               {'ScenarioName':'JacksonMO_01',
                                'ScenarioType':''
                                'ReturnPeriods':['29']
                               },
                               {'ScenarioName':'JacksonMO_02',
                                'ReturnPeriods':['2']
                               }
                           ]
              },
              {'Hazard':'hurricane',
              etc...}
             ]
        """
        print('Finding Hazards, Scenarios, Return Periods...')
        HSRPList = []
        for hazard in self.Hazards:
            hazardDict = {}
            hazardDict['Hazard'] = hazard
            
            scenarioList = self.getScenarios(hazard)
            scenarioDictList = []
            for scenario in scenarioList:
                scenarioDict = {}
                scenarioDict['ScenarioName'] = scenario
                scenarioDict['ReturnPeriods'] = self.getReturnPeriods(hazard, scenario)
                scenarioDictList.append(scenarioDict)
                
            hazardDict['Scenarios'] = scenarioDictList

            HSRPList.append(hazardDict)
            
        self.HazardsScenariosReturnPeriods = HSRPList
        print('...Done')
        print()



    #GET RESULTS
    #earthquakeProbalisticReturnPeriods 8return periods
    #earthquakeProbalisticReturnPeriod
    #earthquakeDeterministic
    #earthquakeAAL?
    
    #floodStudyCases
    #floodStudyCaseRiverineReturnPeriods
    #floodStudyCaseRiverineReturnPeriod
    #floodStudyCaseCoastalPeriods
    #floodStudyCaseCoastalPeriod
    #floodStudyCaseRiverineCoastalPeriods
    #floodStudyCaseRiverineCoastalPeriod
    #floodStudyCaseSurge
    #floodAAL?

    #hurricaneProbalistic 7return periods
    #hurricaneDeterministic
    #hurricaneAAL?

    #tsunami
        
    def getEconomicLoss(self):
        """
        Queries the total economic loss for a study region from the local Hazus SQL Server database
            Returns:
                df: pandas dataframe -- a dataframe of economic loss
        """
        print('getEconomicLoss')
        try:
            # constant to convert to real USD
            constant = 1000
            sqlDict = {
                'earthquake': """select Tract as tract, SUM(ISNULL(TotalLoss, 0)) * {c} as EconLoss from {s}.dbo.[eqTractEconLoss] group by [eqTractEconLoss].Tract""".format(s=self.name, c=constant),
                'flood': """select CensusBlock as block, Sum(ISNULL(TotalLoss, 0))* {c} as EconLoss from {s}.dbo.flFRGBSEcLossByTotal
                    where StudyCaseId = (select StudyCaseID from {s}.[dbo].[flStudyCase] where StudyCaseName = '{sc}')
                    and ReturnPeriodId = '{rp}'
                 group by CensusBlock""".format(s=self.name, c=constant, sc=self.scenario, rp=self.returnPeriod),
                 # NOTE: huSummaryLoss will result in double economic loss. It stores results for occupancy and structure type
                # 'hurricane': """select TRACT as tract, SUM(ISNULL(TotLoss, 0)) * {c} as EconLoss from {s}.dbo.[huSummaryLoss] 
                #     where ReturnPeriod = '{rp} '
                #     and huScenarioName = '{sc}'
                #     group by Tract""".format(s=self.name, c=constant, rp=self.returnPeriod, sc=self.scenario),
                'hurricane': """
                    select TRACT as tract, SUM(ISNULL(Total, 0)) * {c} as EconLoss from {s}.dbo.[hv_huResultsOccAllLossT]
                        where Return_Period = '{rp}' 
                        and huScenarioName = '{sc}'
                        group by Tract
                """.format(s=self.name, c=constant, rp=self.returnPeriod, sc=self.scenario),
                'tsunami': """select CensusBlock as block, SUM(ISNULL(TotalLoss, 0)) * {c} as EconLoss from {s}.dbo.tsuvResDelKTotB group by CensusBlock""".format(s=self.name, c=constant)
            }

            df = self.query(sqlDict[self.hazard])
            return HazusPackageRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getBuildingDamage(self):
        print('getBuildingDamage')
        try:
            constant = 1000
            sqlDict = {
                'earthquake': """SELECT Tract as tract, SUM(ISNULL(PDsNoneBC, 0))
                        As NoDamage, SUM(ISNULL(PDsSlightBC, 0)) AS Affected, SUM(ISNULL(PDsModerateBC, 0))
                        AS Minor, SUM(ISNULL(PDsExtensiveBC, 0)) AS Major,
                        SUM(ISNULL(PDsCompleteBC, 0)) AS Destroyed FROM [{s}].dbo.[eqTractDmg]
                        WHERE DmgMechType = 'STR' group by Tract
                """.format(s=self.name),
                'flood': """SELECT CensusBlock as block, SUM(ISNULL(TotalLoss, 0)) * {c}
                        AS TotalLoss, SUM(ISNULL(BuildingLoss, 0)) * {c} AS BldgLoss,
                        SUM(ISNULL(ContentsLoss, 0)) * {c} AS ContLoss
                        FROM [{s}].dbo.[flFRGBSEcLossBySOccup] 
                        where StudyCaseId = (select StudyCaseID from {s}.[dbo].[flStudyCase] where StudyCaseName = '{sc}')
                        and ReturnPeriodId = '{rp}'
                        GROUP BY CensusBlock
                        """.format(s=self.name, c=constant, sc=self.scenario, rp=self.returnPeriod),
                'hurricane': """SELECT Tract AS tract,
                        SUM(ISNULL(NonDamage, 0)) As NoDamage, SUM(ISNULL(MinDamage, 0)) AS Affected,
                        SUM(ISNULL(ModDamage, 0)) AS Minor, SUM(ISNULL(SevDamage, 0)) AS Major,
                        SUM(ISNULL(ComDamage, 0)) AS Destroyed FROM [{s}].dbo.[huSummaryDamage]
                        WHERE GenBldgOrGenOcc IN('COM', 'AGR', 'GOV', 'EDU', 'REL','RES', 'IND')
                        and ReturnPeriod = '{rp}' 
                        and huScenarioName = '{sc}'
                        GROUP BY Tract""".format(s=self.name, sc=self.scenario, rp=self.returnPeriod),
                'tsunami': """select CBFips as block,
                        ISNULL(count(case when BldgLoss/NULLIF(ValStruct+ValCont, 0) <=0.05 then 1 end), 0) as Affected,
                        ISNULL(count(case when BldgLoss/NULLIF(ValStruct+ValCont, 0) > 0.05 and BldgLoss/(ValStruct+ValCont) <=0.3 then 1 end), 0) as Minor,
                        ISNULL(count(case when BldgLoss/NULLIF(ValStruct+ValCont, 0) > 0.3 and BldgLoss/(ValStruct+ValCont) <=0.5 then 1 end), 0) as Major,
                        ISNULL(count(case when BldgLoss/NULLIF(ValStruct+ValCont, 0) >0.5 then 1 end), 0) as Destroyed
                        from (select NsiID, ValStruct, ValCont  from {s}.dbo.tsHazNsiGbs) haz
                            left join (select NsiID, CBFips from {s}.dbo.tsNsiGbs) gbs
                            on haz.NsiID = gbs.NsiID
                            left join (select NsiID, BldgLoss from {s}.dbo.tsFRNsiGbs) frn
                            on haz.NsiID = frn.NsiID
                            group by CBFips""".format(s=self.name)

            }

            df = self.query(sqlDict[self.hazard])
            return HazusPackageRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getFatalities(self):
        """ Queries the fatalities for a study region from the local Hazus SQL Server database
            Returns:
                df: pandas dataframe -- a dataframe of fatalities
        """
        print('getFatalities')
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

            if (sqlDict[self.hazard] == None) and self.hazard == 'hurricane':
                df = pd.DataFrame(columns=['tract', 'Fatalities'])
            elif (sqlDict[self.hazard] == None) and self.hazard == 'flood':
                df = pd.DataFrame(columns=['block', 'Fatalities'])
            else:
                df = self.query(sqlDict[self.hazard])
            return HazusPackageRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
        
    def getInjuries(self):
        """ Queries the injuries for a study region from the local Hazus SQL Server database
            Returns:
                df: pandas dataframe -- a dataframe of injuries
        """
        print('getInjuries')
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
            if (sqlDict[self.hazard] == None) and self.hazard == 'hurricane':
                df = pd.DataFrame(columns=['tract', 'Injuries'])
            elif (sqlDict[self.hazard] == None) and self.hazard == 'flood':
                df = pd.DataFrame(columns=['block', 'Injuries'])
            else:
                df = self.query(sqlDict[self.hazard])
            return HazusPackageRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
        
    def getShelterNeeds(self):
        """ Queries the short term shelter needs for a study region from the local Hazus SQL Server database
            Returns:
                df: pandas dataframe -- a dataframe of short term shelter needs
        """
        print('getShelterNeeds')
        try:

            # NOTE shelter needs aren't available for the tsunami model - placeholder below
            sqlDict = {
                'earthquake': """select Tract as tract, SUM(ShortTermShelter) as ShelterNeeds from {s}.dbo.eqTract group by Tract""".format(s=self.name),
                'flood': """select CensusBlock as block, SUM(ShortTermNeeds) as ShelterNeeds from {s}.dbo.flFRShelter
                    where StudyCaseId = (select StudyCaseID from {s}.[dbo].[flStudyCase] where StudyCaseName = '{sc}')
                    and ReturnPeriodId = '{rp}'
                    group by CensusBlock""".format(s=self.name, sc=self.scenario, rp=self.returnPeriod),
                'hurricane': """select TRACT as tract, SUM(SHORTTERMSHELTERNEEDS) as ShelterNeeds from {s}.dbo.huShelterResultsT
                    where Return_Period = '{rp}' 
                    and huScenarioName = '{sc}'
                     group by Tract
                        """.format(s=self.name, sc=self.scenario, rp=self.returnPeriod),
                'tsunami': None
            }
            if (sqlDict[self.hazard] == None) and self.hazard == 'tsunami':
                df = pd.DataFrame(columns=['block', 'ShelterNeeds'])
            else:
                df = self.query(sqlDict[self.hazard])
            return HazusPackageRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getDisplacedHouseholds(self):
        """ Queries the displaced households for a study region from the local Hazus SQL Server database
            Returns:
                df: pandas dataframe -- a dataframe of displaced households
        """
        print('getDisplacedHouseholds')
        try:

            # TODO check to see if flood is displaced households or population -- database says pop
            # NOTE displaced households not available in tsunami model - placeholder below
            sqlDict = {
                'earthquake': """select Tract as tract, SUM(DisplacedHouseholds) as DisplacedHouseholds from {s}.dbo.eqTract group by Tract""".format(s=self.name),
                'flood': """select CensusBlock as block, SUM(DisplacedPop) as DisplacedHouseholds from {s}.dbo.flFRShelter
                    where StudyCaseId = (select StudyCaseID from {s}.[dbo].[flStudyCase] where StudyCaseName = '{sc}')
                    and ReturnPeriodId = '{rp}'
                    group by CensusBlock""".format(s=self.name, sc=self.scenario, rp=self.returnPeriod),
                'hurricane': """select TRACT as tract, SUM(DISPLACEDHOUSEHOLDS) as DisplacedHouseholds from {s}.dbo.huShelterResultsT
                        where Return_Period = '{rp}' 
                        and huScenarioName = '{sc}'
                    group by Tract""".format(s=self.name, sc=self.scenario, rp=self.returnPeriod),
                'tsunami': None
            }

            if (sqlDict[self.hazard] == None) and self.hazard == 'tsunami':
                df = pd.DataFrame(columns=['block', 'DisplacedHouseholds'])
            else:
                df = self.query(sqlDict[self.hazard])
            return HazusPackageRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getDebris(self):
        """ Queries the debris for a study region from the local Hazus SQL Server database
            Returns:
                df: pandas dataframe -- a dataframe of debris
        """
        print('getDebris')
        try:
            constant = 1000
            # NOTE debris not available for tsunami model - placeholder below
            # NOTE hurricane is the only model NOT in thousands of tons. It doesn't need to be multipled by the constant
            sqlDict = {
                'earthquake': """select Tract as tract, SUM(DebrisW) * {c} as DebrisBW, SUM(DebrisS) * {c} as DebrisCS, SUM(DebrisTotal) * {c} as DebrisTotal from {s}.dbo.eqTract group by Tract""".format(s=self.name, c=constant),
                'flood': """select CensusBlock as block, SUM(FinishTons) * {c} as DebrisTotal from {s}.dbo.flFRDebris
                    where StudyCaseId = (select StudyCaseID from {s}.[dbo].[flStudyCase] where StudyCaseName = '{sc}')
                    and ReturnPeriodId = '{rp}'
                    group by CensusBlock""".format(s=self.name, c=constant, sc=self.scenario, rp=self.returnPeriod),
                'hurricane': """select d.tract, d.DebrisTotal, d.DebrisBW, d.DebrisCS, d.DebrisTree, (d.DebrisTree * p.TreeCollectionFactor) as DebrisEligibleTree from
                    (select Tract as tract, SUM(BRICKANDWOOD) as DebrisBW, SUM(CONCRETEANDSTEEL) as DebrisCS, SUM(Tree) as DebrisTree, SUM(BRICKANDWOOD + CONCRETEANDSTEEL + Tree) as DebrisTotal from {s}.dbo.huDebrisResultsT
                        where Return_Period = '{rp}'
                        and huScenarioName = '{sc}'
                        group by Tract) d
                        inner join (select Tract as tract, TreeCollectionFactor from {s}.dbo.huTreeParameters) p
                        on d.tract = p.tract
                """.format(s=self.name, sc=self.scenario, rp=self.returnPeriod),
                'tsunami': """select CensusBlock as block, SUM(FinishTons) * {c} as DebrisTotal from {s}.dbo.flFRDebris group by CensusBlock""".format(s=self.name, c=constant)
            }

            df = self.query(sqlDict[self.hazard])
            return HazusPackageRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
        
    def getDemographics(self):
        """Summarizes demographics at the lowest level of geography
            Returns:
                df: pandas dataframe -- a dataframe of the summarized demographics
        """
        print('getDemographics')
        try:

            sqlDict = {
                'earthquake': """select Tract as tract, Population, Households FROM {s}.dbo.[hzDemographicsT]""".format(s=self.name),
                'flood': """select CensusBlock as block, Population, Households FROM {s}.dbo.[hzDemographicsB]""".format(s=self.name),
                'hurricane': """select Tract as tract, Population, Households FROM {s}.dbo.[hzDemographicsT]""".format(s=self.name),
                'tsunami': """select CensusBlock as block, Population, Households FROM {s}.dbo.[hzDemographicsB]""".format(s=self.name)
            }

            df = self.query(sqlDict[self.hazard])
            return HazusPackageRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
    
    def getResults(self):
        """ Summarizes results at the lowest level of geography
            Returns:
                df: pandas dataframe -- a dataframe of the summarized results
        """
        print('getResults...')
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

            return HazusPackageRegionDataFrame(self, df)
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

            # TODO should tsunami be ts or eq? tsunami doesn't appear to contain essential facilities
            prefixDict = {
                'earthquake': 'eq',
                'hurricane': 'huResults',
                'flood': 'flFR',
                'tsunami': 'ts'
            }
            prefix = prefixDict[self.hazard]

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
                        tempColumns = [x.replace(x, '['+x+']') for x in hzcolumns]
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
                        # rename minor/moderate/severe/complete
                        srcolumns = [x.replace('MINOR', 'MINOR as Affected') for x in srcolumns]
                        srcolumns = [x.replace('MODERATE', 'MODERATE as Minor') for x in srcolumns]
                        srcolumns = [x.replace('SEVERE', 'SEVERE as Major') for x in srcolumns]
                        srcolumns = [x.replace('COMPLETE', 'COMPLETE as Destroyed') for x in srcolumns]
                        hzcolumnsFinal = ', '.join(
                            ['hz.' + x for x in hzcolumns])
                        srcolumnsFinal = ', '.join(
                            ['sr.' + x for x in srcolumns])
                        querysetColumns = ', '.join(
                            [srcolumnsFinal, hzcolumnsFinal])

                        # change to real dollars
                        if 'sr.EconLoss' in querysetColumns:
                            querysetColumns = querysetColumns.replace(
                                'sr.EconLoss', 'sr.EconLoss * 1000 as EconLoss')

                        # build where clause
                        whereClauseDict = {
                            'earthquake': """where EconLoss > 0""",
                            'flood': """where StudyCaseId = (select StudyCaseID from {s}.[dbo].[flStudyCase] where StudyCaseName = '{sc}') and ReturnPeriodId = '{rp}'""".format(s=self.name, sc=self.scenario, rp=self.returnPeriod),
                            'hurricane': """where Return_Period = '{rp}' and huScenarioName = '{sc}'""".format(sc=self.scenario, rp=self.returnPeriod),
                            'tsunami': """where EconLoss > 0"""
                        }
                        whereClause = whereClauseDict[self.hazard]

                        # build dynamic sql query
                        sql = """
                                SELECT
                                    {qc}
                                    FROM
                                    (SELECT
                                        {src}
                                        from [{s}].[dbo].[{p}{f}]
                                        {wc}) sr
                                    left join
                                    (SELECT
                                        {hzc}
                                        from [{s}].[dbo].[hz{f}]) hz
                                    on hz.FacilityID = sr.FacilityID
                                """.format(i=idColumn, s=self.name, f=facility, p=prefix, qc=querysetColumns, src=studyRegionColumns, hzc=hazusColumns, wc=whereClause)

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
                return HazusPackageRegionDataFrame(self, essentialFacilityDf)
            else:
                print("No essential facility loss information for " +
                      self.name)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
        
    def getBuildingDamageByOccupancy(self):
        """ Queries the building damage by occupancy type for a study region from the local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of building damage by occupancy type
        """
        try:
            constant = 1000
            sqlDict = {
                'earthquake': """SELECT Occupancy, SUM(ISNULL(PDsNoneBC, 0))
                        As NoDamage, SUM(ISNULL(PDsSlightBC, 0)) AS Affected, SUM(ISNULL(PDsModerateBC, 0))
                        AS Minor, SUM(ISNULL(PDsExtensiveBC, 0)) AS Major,
                        SUM(ISNULL(PDsCompleteBC, 0)) AS Destroyed FROM {s}.dbo.[eqTractDmg]
                        WHERE DmgMechType = 'STR' GROUP BY Occupancy""".format(s=self.name),
                'flood': """SELECT SOccup AS Occupancy, SUM(ISNULL(TotalLoss, 0)) * {c}
                        AS TotalLoss, SUM(ISNULL(BuildingLoss, 0)) * {c} AS BldgLoss,
                        SUM(ISNULL(ContentsLoss, 0)) * {c} AS ContLoss
                        FROM {s}.dbo.[flFRGBSEcLossBySOccup]
                        where StudyCaseId = (select StudyCaseID from {s}.[dbo].[flStudyCase] where StudyCaseName = '{sc}')
                        and ReturnPeriodId = '{rp}'
                        GROUP BY SOccup
                        """.format(s=self.name, c=constant, sc=self.scenario, rp=self.returnPeriod),
                'hurricane': """SELECT GenBldgOrGenOcc AS Occupancy,
                        SUM(ISNULL(NonDamage, 0)) As NoDamage, SUM(ISNULL(MinDamage, 0)) AS Affected,
                        SUM(ISNULL(ModDamage, 0)) AS Minor, SUM(ISNULL(SevDamage, 0)) AS Major,
                        SUM(ISNULL(ComDamage, 0)) AS Destroyed FROM {s}.dbo.[huSummaryDamage]
                        WHERE GenBldgOrGenOcc IN('COM', 'AGR', 'GOV', 'EDU', 'REL','RES', 'IND')
                        and ReturnPeriod = '{rp}'
                        and huScenarioName = '{sc}'
                        GROUP BY GenBldgOrGenOcc""".format(s=self.name, sc=self.scenario, rp=self.returnPeriod),
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

            df = self.query(sqlDict[self.hazard])
            return HazusPackageRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def getBuildingDamageByType(self):
        """ Queries the building damage by structure type for a study region from the local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of building damage by structure type
        """
        try:
            constant = 1000
            sqlDict = {
                'earthquake': """SELECT eqBldgType AS BldgType,
                        SUM(ISNULL(PDsNoneBC, 0)) As NoDamage, SUM(ISNULL(PDsSlightBC, 0)) AS Affected,
                        SUM(ISNULL(PDsModerateBC, 0)) AS Minor, SUM(ISNULL(PDsExtensiveBC, 0))
                        AS Major, SUM(ISNULL(PDsCompleteBC, 0)) AS Destroyed
                        FROM {s}.dbo.[eqTractDmg] WHERE DmgMechType = 'STR'
                        GROUP BY eqBldgType""".format(s=self.name),
                'flood': """SELECT BldgType, SUM(ISNULL(TotalLoss, 0)) * {c} AS TotalLoss,
                        SUM(ISNULL(BuildingLoss, 0)) * {c} AS BldgLoss, SUM(ISNULL(ContentsLoss, 0)) * {c} AS ContLoss
                        FROM {s}.dbo.[flFRGBSEcLossByGBldgType] 
                        where StudyCaseId = (select StudyCaseID from {s}.[dbo].[flStudyCase] where StudyCaseName = '{sc}')
                        and ReturnPeriodId = '{rp}'
                        GROUP BY BldgType""".format(s=self.name, c=constant, sc=self.scenario, rp=self.returnPeriod),
                'hurricane': """SELECT GenBldgOrGenOcc AS Occupancy,
                        SUM(ISNULL(NonDamage, 0)) As NoDamage, SUM(ISNULL(MinDamage, 0)) AS Affected,
                        SUM(ISNULL(ModDamage, 0)) AS Minor, SUM(ISNULL(SevDamage, 0)) AS Major,
                        SUM(ISNULL(ComDamage, 0)) AS Destroyed FROM {s}.dbo.[huSummaryDamage]
                        WHERE GenBldgOrGenOcc IN('CONCRETE', 'MASONRY', 'STEEL', 'WOOD', 'MH')
                        and ReturnPeriod = '{rp}' 
                        and huScenarioName = '{sc}'
                        GROUP BY GenBldgOrGenOcc""".format(s=self.name, sc=self.scenario, rp=self.returnPeriod),
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

            df = self.query(sqlDict[self.hazard])
            return HazusPackageRegionDataFrame(self, df)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    #EXPORT DATA

    

