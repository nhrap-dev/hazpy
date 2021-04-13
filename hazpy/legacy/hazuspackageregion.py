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

from .studyregiondataframe import StudyRegionDataFrame
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
        self.hprFilePath = Path(hprFilePath)
        self.outputDir = Path(outputDir)
        self.tempDir = Path.joinpath(self.outputDir, self.hprFilePath.stem + '_temp')

        self.hprComment = self.getHPRComment(self.hprFilePath)
        self.HazusVersion = self.getHPRHazusVersion(self.hprComment)
        self.Hazards = self.getHPRHazards(self.hprComment)
        
        self.floodStudyCases = []
        self.floodStudyCaseRiverineReturnPeriods = []
        self.floodStudyCaseCoastalPeriods = []
        self.floodStudyCaseRiverineCoastalPeriods = []

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
        elif len(bkList) == 1:
            print()
            bkFilePath = Path.joinpath(fileDir, str(bkList[0]))
            self.bkFilePath = bkFilePath
            self.dbName = bkFilePath.stem
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
        """
        """
        #unzipHPR...
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
        
    #GET DATA
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

    
    #CLEANUP
    def detachDB(self, dbName, cursor):
        """
        """
        print(f'Detaching {dbName}...')
        ##crsr.execute(f"USE [master] ALTER DATABASE [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE") #may help with locks
        cursor.execute(f"USE [master] EXEC master.dbo.sp_detach_db @dbname = N'bk_{dbName}'")
        while cursor.nextset():
            pass
        print('...done')
        print()
        
    def deleteDIR(self, dirPath):
        """
        """
        print(f'Deleting temp folder:{dirPath}...')
        try:
            shutil.rmtree(dirPath)
        except OSError as e:
            print ("Error: %s - %s." % (e.filename, e.strerror))
        print('...done')
        print()
