import pandas as pd
import geopandas as gpd
import sys


class StudyRegionDataFrame(pd.DataFrame):
    """ -- StudyRegion helper class --
        Intializes a study region dataframe class - A pandas dataframe extended with extra methods


        Key Argument:
            studyRegionClass: StudyRegion -- an initialized StudyRegion class
            df: pandas dataframe -- a dataframe to extend as a StudyRegionDataFrame

    """

    def __init__(self, studyRegionClass, df):
        super().__init__(df)
        try:
            self.studyRegion = studyRegionClass.name
        except:
            self.studyRegion = studyRegionClass.studyRegion
        self.conn = studyRegionClass.conn
        self.query = studyRegionClass.query

    def addCensusTracts(self):
        """ Queries the census tract geometry for a study region in local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of the census geometry and fips codes
        """
        try:

            sql = """SELECT Tract as tract, Shape.STAsText() AS geometry FROM {s}.dbo.hzTract""".format(
                s=self.studyRegion)

            df = self.query(sql)
            newDf = pd.merge(df, self, on="tract")
            return StudyRegionDataFrame(self, newDf)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def addCensusBlocks(self):
        """ Queries the census block geometry for a study region in local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of the census geometry and fips codes
        """
        try:

            sql = """SELECT CensusBlock as block, Shape.STAsText() AS geometry FROM {s}.dbo.hzCensusBlock""".format(
                s=self.studyRegion)

            df = self.query(sql)
            newDf = pd.merge(df, self, on="block")
            return StudyRegionDataFrame(self, newDf)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def addCounties(self):
        """ Queries the county geometry for a study region in local Hazus SQL Server database

            Returns:
                df: pandas dataframe -- a dataframe of the census geometry and fips codes
        """
        try:

            sql = """SELECT CountyFips as county, CountyName as name, Shape.STAsText() AS geometry FROM {s}.dbo.hzCounty""".format(
                s=self.studyRegion)

            df = self.query(sql)
            newDf = pd.merge(df, self, on="county")
            return StudyRegionDataFrame(self, newDf)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def addGeometry(self):
        """ Adds geometry to any HazusDB class dataframe with a census block, tract, or county id

            Key Argument:
                dataframe: pandas dataframe -- a HazusDB generated dataframe with a fips column named either block, tract, or county
            Returns:
                df: pandas dataframe -- a copy of the input dataframe with the geometry added
        """
        try:
            if 'block' in self.columns:
                df = self.addCensusBlocks()
                return StudyRegionDataFrame(self, df)
            elif 'tract' in self.columns:
                df = self.addCensusTracts()
                return StudyRegionDataFrame(self, df)
            elif 'county' in self.columns:
                df = self.addCounties()
                return StudyRegionDataFrame(self, df)
            else:
                print(
                    'Unable to find the column name block, tract, or county in the dataframe input')
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def toCSV(self, path):
        """ Exports a StudyRegionDataFrame to a CSV

            Key Argument:
                path: str -- the output directory path, file name, and extention (example: 'C:/directory/filename.csv')
        """
        self.to_csv(path, index=False)

    def toShapefile(self, path):
        """ Exports a StudyRegionDataFrame to an Esri Shapefile

            Key Argument:
                path: str -- the output directory path, file name, and extention (example: 'C:/directory/filename.shp')
        """
        try:
            if 'geometry' not in self.columns:
                self = self.addGeometry()
            self['geometry'] = self['geometry'].apply(
                lambda x: loads(x))
            gdf = gpd.GeoDataFrame(self, geometry='geometry')
            gdf.to_file(path, driver='ESRI Shapefile')
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def toGeoJSON(self, path):
        """ Exports a StudyRegionDataFrame to a web compatible GeoJSON

            Key Argument:
                path: str -- the output directory path, file name, and extention (example: 'C:/directory/filename.geojson')
        """
        try:
            if 'geometry' not in self.columns:
                self = self.addGeometry()
            self['geometry'] = self['geometry'].apply(lambda x: loads(x))
            gdf = gpd.GeoDataFrame(self, geometry='geometry')
            for index in range(len(gdf['geometry'])):
                if type(gdf['geometry'][index]) == Polygon:
                    gdf['geometry'][index] = MultiPolygon(
                        [gdf['geometry'][index]])
            # gdf['geometry'].apply(orient, args=(1,))
            gdf.to_file(path, driver='GeoJSON')
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
