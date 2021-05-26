from re import template
import PyPDF2
from PyPDF2.merger import PdfFileMerger
from xhtml2pdf import pisa
import os
import pandas as pd

import geopandas as gpd
from shapely.wkt import loads

from matplotlib import pyplot as plt
from mpl_toolkits.axes_grid1.axes_divider import make_axes_locatable
import matplotlib.patheffects as pe
import matplotlib.ticker as ticker
import seaborn as sns
import shapely
import numpy as np
import shutil
### Added from hazus reports
import rasterio as rio
from time import time
import pdfrw
from reportlab.pdfgen import canvas
from reportlab.graphics import renderPDF
from PyPDF2 import PdfFileWriter, PdfFileReader
from jenkspy import jenks_breaks as nb
from copy import copy
from matplotlib.patches import Polygon
import requests
from matplotlib.collections import PatchCollection
import datetime
from PIL import Image
import sys
from uuid import uuid4 as uuid
from reportlab.pdfgen import canvas
from PyPDF2.generic import BooleanObject, NameObject, IndirectObject

class Report():
    """ -- A StudyRegion helper class --
    Creates a report object. Premade reports are exportable using the save method and 
    specifying the report in the parameter premade. The Report class can also be used as an API 
    to create reports using the addTable, addHistogram, and addMap methods.

    Keyword Arguments: \n
        title: str -- report title
        subtitle: str -- report subtitle
        icon: str -- report hazard icon (choices: 'earthquake', 'flood', 'hurricane', 'tsunami')

    """

    def __init__(self, studyRegionClass, title, subtitle, icon):
        self.__getResults = studyRegionClass.getResults
        self.__getBuildingDamageByOccupancy = studyRegionClass.getBuildingDamageByOccupancy
        self.__getBuildingDamageByType = studyRegionClass.getBuildingDamageByType
        self.__getEssentialFacilities = studyRegionClass.getEssentialFacilities
        self.__getHazardGeoDataFrame = studyRegionClass.getHazardGeoDataFrame
        self.__getTravelTimeToSafety = studyRegionClass.getTravelTimeToSafety
        self.__getInjuries = studyRegionClass.getInjuries
        self.__getFatalities = studyRegionClass.getFatalities
        self.hazard = studyRegionClass.hazard
        self.scenario = studyRegionClass.scenario
        self.returnPeriod = studyRegionClass.returnPeriod
        self.assets = {
            'earthquake': 'https://fema-ftp-snapshot.s3.amazonaws.com/Hazus/Assets/hazard_icons/Earthquake_DHSGreen.png',
            'flood': 'https://fema-ftp-snapshot.s3.amazonaws.com/Hazus/Assets/hazard_icons/Flood_DHSGreen.png',
            'hurricane': 'https://fema-ftp-snapshot.s3.amazonaws.com/Hazus/Assets/hazard_icons/Hurricane_DHSGreen.png',
            'tsunami': 'https://fema-ftp-snapshot.s3.amazonaws.com/Hazus/Assets/hazard_icons/Tsunami_DHSGreen.png',
            'tornado': 'https://fema-ftp-snapshot.s3.amazonaws.com/Hazus/Assets/hazard_icons/Tornado_DHSGreen.png',
            'hazus': 'https://fema-ftp-snapshot.s3.amazonaws.com/Hazus/Assets/hazus_icons/hazus_cropped.png'
        }

        self.columnLeft = ''
        self.columnRight = ''
        self.title = title
        self.subtitle = subtitle
        self.icon = self.assets[icon]
        self.templateFillableLocation = 'templates'
        self.disclaimer = """The estimates of social and economic impacts contained in this report were produced using Hazus loss estimation methodology software which is based on current scientific and engineering knowledge. There are uncertainties inherent in any loss estimation
            technique. Therefore, there may be significant differences between the modeled results contained in this report and the actual social and economic losses following a specific earthquake. These results can be improved by using enhanced inventory, geotechnical,
            and observed ground motion data."""
        self.getCounties = studyRegionClass.getCounties
        self._tempDirectory = 'hazpy-report-temp'

    def abbreviate(self, number):
        try:
            digits = 0
            number = float(number)
            formattedString = str("{:,}".format(round(number, digits)))
            if ('.' in formattedString) and (digits == 0):
                formattedString = formattedString.split('.')[0]
            if (number > 1000) and (number < 1000000):
                split = formattedString.split(',')
                formattedString = split[0] + '.' + split[1][0:-1] + ' K'
            if (number > 1000000) and (number < 1000000000):
                split = formattedString.split(',')
                formattedString = split[0] + '.' + split[1][0:-1] + ' M'
            if (number > 1000000000) and (number < 1000000000000):
                split = formattedString.split(',')
                formattedString = split[0] + '.' + split[1][0:-1] + ' B'
            if (number > 1000000000000) and (number < 1000000000000000):
                split = formattedString.split(',')
                formattedString = split[0] + '.' + split[1][0:-1] + ' T'
            return formattedString
        except:
            return str(number)

    def addCommas(self, number, abbreviate=False, truncate=False):
        if truncate:
            number = int(round(number))
        if abbreviate:
            number = self.abbreviate(number)
        else:
            number = "{:,}".format(number)
        return number

    def toDollars(self, number, abbreviate=False, truncate=False):
        if truncate:
            number = int(round(number))
        if abbreviate:
            dollars = self.abbreviate(number)
            dollars = '$' + dollars
        else:
            dollars = '$'+"{:,}".format(number)
            dollarsSplit = dollars.split('.')
            if len(dollarsSplit) > 1:
                dollars = '.'.join([dollarsSplit[0], dollarsSplit[1][0:1]])
        return dollars

    def updateTemplate(self):
        self.template = """
            <html>
                <head>
                    <style>
                        @page {
                            size: a4 portrait;
                            @frame header_frame {
                                /*Static Frame*/ 
                                -pdf-frame-content: header_content;
                                left: 50pt;
                                width: 512pt;
                                top: 50pt;
                                height: 40pt;
                            }
                            @frame content_frame {
                                /*Content Frame*/
                                left: 20px;
                                right: 20px;
                                top: 20px;
                                bottom: 20px;
                            }
                            @frame footer_frame {
                                /*Another static Frame*/
                                -pdf-frame-content: footer_content;
                                left: 50pt;
                                width: 512pt;
                                top: 772pt;
                                height: 20pt;
                            }
                        }
                        .header_border {
                            font-size: 3px;
                            width: 512pt;
                            background-color: #0078a9;
                            color: #0078a9;
                            padding-top: 0;
                            padding-bottom: 0;
                            padding-left: 0;
                            padding-right: 0;
                        }
                        .header {
                            width: 512pt;
                            border: 2px solid #abadb0;
                            margin-top: 5px;
                            margin-bottom: 5px;
                            padding-top: 10px;
                            padding-bottom: 10px;
                            padding-left: 10px;
                            padding-right: 10px;
                        }
                        .header_table_cell_icon {
                            border: none;
                            width: 100px;
                            padding-top: 5px;
                            padding-bottom: 5px;
                            padding-left: 10px;
                            padding-right: 0;
                        }
                        .header_table_cell_icon_img {
                            width: auto;
                            height: 60px;
                        }
                        .header_table_cell_text {
                            border: none;
                            width: 50%;
                            text-align: left;
                            margin-left: 20px;
                            margin-left: 20px;
                        }
                        .header_table_cell_logo {
                            padding-top: 0;
                            padding-bottom: 0;
                            padding-left: 35px;
                            padding-right: 0;
                            border: none;
                        }
                        .header_table_cell_logo_img {
                            width: auto;
                            height: 40px;
                        }
                        .header_title {
                            font-size: 16px;
                            padding-top: 10px;
                            padding-bottom: 0;
                            padding-left: 0;
                            padding-right: 0;
                            margin-top: 10px;
                            margin-bottom: 0;
                            margin-left: 0;
                            margin-right: 0;

                        }
                        .header_subtitle {
                            font-size: 12px;
                            padding-top: 0;
                            padding-bottom: 0;
                            padding-left: 0;
                            padding-right: 0;
                            margin-top: 0;
                            margin-bottom: 0;
                            margin-left: 0;
                            margin-right: 0;
                        }
                        .column_left {
                            margin-top: 0;
                            padding-top: 5px;
                            padding-bottom: 0;
                            padding-left: 0;
                            padding-right: 5px;
                            height: 690pt;
                            vertical-align: top;
                        }
                        .column_right {
                            margin-top: 0;
                            padding-top: 5px;
                            padding-bottom: 0;
                            padding-left: 5px;
                            padding-right: 0;
                            height: 690pt;
                            vertical-align: top;
                        }
                        .report_columns {
                            padding-top: 5px;
                            padding-bottom: 5px;
                        }
                        .result_container {
                            padding-top: 0;
                            padding-bottom: 0;
                            padding-left: 0;
                            padding-right: 0;
                        }
                        .result_container_spacer {
                            font-size: 2px;
                            width: 100%;
                            background-color: #fff;
                            color: #fff;
                            padding-top: 0;
                            padding-bottom: 0;
                            padding-left: 0;
                            padding-right: 0;
                            margin-top: 0;
                            margin-bottom: 0;
                            margin-left: 0;
                            margin-right: 0;
                        }
                        .results_table {
                            height: auto;
                            width: 100%;
                            padding-top: 0;
                            padding-bottom: 0;
                            padding-left: 0;
                            padding-right: 0;
                            margin-top: 0;
                            margin-bottom: 0;
                            margin-left: 0;
                            margin-right: 0;
                        }
                        .results_header {
                            background-color: #0078a9;
                            color: #000;
                        }
                        .results_table_header {
                            background-color: #0078a9;
                            margin-bottom: 0;
                            padding-top: 3px;
                            padding-bottom: 1px;
                        }
                        .results_table_header_title {
                            color: #fff;
                            text-align: left;
                            padding-top: 3px;
                            padding-bottom: 1px;
                            padding-right: 1px;
                            padding-left: 5px;
                            width: 40%;
                        }
                        .results_table_header_title_solo {
                            color: #fff;
                            text-align: left;
                            padding-top: 3px;
                            padding-bottom: 1px;
                            padding-left: 5px;
                            width: 100%;
                        }
                        .results_table_header_total {
                            color: #fff;
                            text-align: right;
                            vertical-align: top;
                            padding-top: 3px;
                            padding-bottom: 1px;
                            padding-right: 1px;
                            padding-left: 0px;
                        }
                        .results_table_header_number {
                            color: #fff;
                            text-align: left;
                            padding-top: 3px;
                            padding-bottom: 1px;
                            padding-right: 1px;
                            padding-left: 0px;
                        }
                        .results_table_cells_header {
                            background-color: #abadb0;
                            color: #fff;
                            border: 1px solid #fff;
                            margin-top: 0;
                            padding-top: 3px;
                            padding-bottom: 1px;
                        }
                        .results_table_cells {
                            background-color: #f9f9f9;
                            border: 1px solid #fff;
                            color: #000;
                            text-align: left;
                            padding-top: 3px;
                            padding-bottom: 1px;
                            padding-left: 5px;
                        }
                        .results_table_img {
                            width: 512pt;
                            height: auto;
                        }
                        .disclaimer {
                            color: #c3c3c3;
                            font-size: 6pt;
                        }
                    </style>
                </head>
                <body>
                    <div id="content_frame">
                        <div class="header_border">_</div>
                        <div class="header">
                            <table>
                            <tr>
                                <td class="header_table_cell_icon">
                                <img
                                    class="header_table_cell_icon_img"
                                    src='"""+self.icon+"""'
                                    alt="hazard"
                                />
                                </td>
                                <td class="header_table_cell_text">
                                    <h1 class="header_title">"""+self.title+"""</h1>
                                    <p class="header_subtitle">"""+self.subtitle+"""</p>
                                </td>
                                <td class="header_table_cell_logo">
                                <img
                                    class="header_table_cell_logo_img"
                                    src='"""+self.assets['hazus']+"""'
                                    alt="hazus"
                                />
                                </td>
                            </tr>
                            </table>
                        </div>
                        <div class="header_border">_</div>
                        <table class="report_columns">
                            <tr>
                                <td class="column_left">
                                """+self.columnLeft+"""
                                </td>
                                <td class="column_right">
                                """+self.columnRight+"""
                                </td>
                            </tr>
                        </table>
                        <p class="disclaimer">"""+self.disclaimer+"""</p>
                    </div>
                </body>
            </html>
            """

    def addTable(self, df, title, total, column):
        """ Adds a table to the report

        Keyword Arguments: \n
            df: pandas dataframe -- expects a StudyRegionDataFrame
            title: str -- section title
            total: str -- total callout box value
            column: str -- which column in the report to add to (options: 'left', 'right')
        """
        headers = ['<tr>']
        for col in df.columns:
            headers.append(
                '<th class="results_table_cells_header">'+col+'</th>')
        headers.append('</tr')
        headers = ''.join(headers)

        values = []
        for index in range(len(df)):
            row = ['<tr>']
            for col in df.columns:
     #           print('These are the value rows: {}'.format(col))
                row.append('<td class="results_table_cells">' +
                           str(df.iloc[index][col])+'</td>')
            row.append('</tr>')
            values.append(''.join(row))
        values = ''.join(values)
        template = """
            <div class="result_container">
                <table class="results_table">
                    <tr class="results_table_header">
                        <th class="results_table_header_title">
                            """+title+"""
                        </th>
                        <th class="results_table_header_total">Total:</th>
                        <th class="results_table_header_number">"""+total+"""</th>
                    </tr>
                </table>
                <table class="results_table">
                    """+headers+"""
                    """+values+"""
                </table>
            </div>
            <div class="result_container_spacer">_</div>
        """
        if column == 'left':
            self.columnLeft = self.columnLeft + template
        if column == 'right':
            self.columnRight = self.columnRight + template

# TODO: Check to see if addImage is used anywhere - does not have any references - BC
    def addImage(self, src, title, column):
        """ Adds image block to the report

        Keyword Arguments: \n
            src: str -- the path and filename of the image
            title: str -- the title of the image
            column: str -- which column in the report to add to (options: 'left', 'right')
        """
        template = """
            <div class="result_container">
                <table class="results_table">
                <tr class="results_table_header">
                    <th class="results_table_header_title_solo">
                    """+title+"""
                    </th>
                </tr>
                </table>
                <img
                class="results_table_img"
                src='"""+src+"""'
                alt='"""+title+"""'
                />
            </div>
            <div class="result_container_spacer">_</div>
            """

    def addMap(self, gdf, field, title, column, legend=False, formatTicks=True, cmap='Blues'):
        """ Adds a map to the report

        Keyword Arguments: \n
            gdf: geopandas geodataframe -- a geodataframe containing the data to be mapped
            field: str -- the field for the choropleth
            title: str -- section title in the report
            column: str -- which column in the report to add to (options: 'left', 'right')
            legend (optional): bool -- adds a colorbar to the map
            formatTicks (optional): bool -- if True, it will abbreviate and add commas to tick marks
            cmap (optional): str -- the colormap used for the choropleth; default = 'Blues'
        """
        try:
            fig = plt.figure(figsize=(3, 3), dpi=300)
            ax = fig.gca()
            ax2 = fig.gca()

            if type(gdf) != gpd.GeoDataFrame:
                gdf['geometry'] = gdf['geometry'].apply(str)
                gdf['geometry'] = gdf['geometry'].apply(loads)
                gdf = gpd.GeoDataFrame(gdf, geometry='geometry')
            try:
                gdf.plot(column=field, cmap=cmap, ax=ax)
            except:
                gdf['geometry'] = gdf['geometry'].apply(loads)
                gdf.plot(column=field, cmap=cmap, ax=ax)

            if legend == True:
                sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=gdf[field].min(), vmax=gdf[field].max()))
                sm._A = []

                divider = make_axes_locatable(ax)
                cax = divider.append_axes("top", size="10%", pad="20%")
                cb = fig.colorbar(sm, cax=cax, orientation="horizontal")
                cb.outline.set_visible(False)
                if formatTicks == True:
                    cb.ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: self.addCommas(x, abbreviate=True, truncate=True)))

                counties = self.getCounties()
                # reduce counties to those that intersect the results
                intersect = counties.intersects(gdf.geometry)
                counties = counties[intersect]

                gdf['dissolve'] = 1
                mask = gdf.dissolve(by='dissolve').envelope
                mask = mask.buffer(0)
                counties['geometry'] = counties.buffer(0)
                counties = gpd.clip(counties, mask)
                counties.plot(facecolor="none", edgecolor="darkgrey", linewidth=0.2, ax=ax2)
                # counties.plot(facecolor="none", edgecolor="darkgrey", linewidth=0.2, ax=ax2)
                annotationDf = counties.sort_values('size', ascending=False)[0:5]
                annotationDf = annotationDf.sort_values('size', ascending=True)

                annotationDf['centroid'] = [x.centroid for x in annotationDf['geometry']]

                maxSize = annotationDf['size'].max()
                topFontSize = 2.5
                annotationDf['fontSize'] = topFontSize * (annotationDf['size'] / annotationDf['size'].max()) + (topFontSize - ((annotationDf['size'] / annotationDf['size'].max()) * 2))
                for row in range(len(annotationDf)):
                    name = annotationDf.iloc[row]['name']
                    coords = annotationDf.iloc[row]['centroid']
                    ax.annotate(s=name, xy=(float(coords.x), float(coords.y)), horizontalalignment='center',
                                size=annotationDf.iloc[row]['fontSize'], color='white', path_effects=[pe.withStroke(linewidth=1, foreground='#404040')])

            fontsize = 3
            for idx in range(len(fig.axes)):
                fig.axes[idx].tick_params(labelsize=fontsize, size=fontsize)

            ax.axis('off')
            ax.axis('scaled')
            ax.autoscale(enable=True, axis='both', tight=True)
            if not os.path.isdir(os.getcwd() + '\\' + self._tempDirectory):
                os.mkdir(os.getcwd() + '/' + self._tempDirectory)
            src = os.getcwd() + '/' + self._tempDirectory + '/'+ str(uuid())+ ".png"
            fig.savefig(src, pad_inches=0, bbox_inches='tight', dpi=600)
            fig.clf()
            plt.clf()

            template = """
                <div class="result_container">
                    <table class="results_table">
                    <tr class="results_table_header">
                        <th class="results_table_header_title_solo">
                        """+title+"""
                        </th>
                    </tr>
                    </table>
                    <img
                    class="results_table_img"
                    src='"""+src+"""'
                    alt='"""+title+"""'
                    />
                </div>
                <div class="result_container_spacer">_</div>
                """
            if column == 'left':
                self.columnLeft = self.columnLeft + template
            if column == 'right':
                self.columnRight = self.columnRight + template
            # Convert PNG to PDF
            title = 'map-' + title
            if self.hazard == 'flood':
                scale = 5.96
                if title == 'map-Economic Loss by Census Block':
                    x, y = 327 * scale, 430 * scale
                if title == 'map-Water Depth (ft) - 100-year':
                    x, y = 327 * scale, 127 * scale
            if self.hazard == 'earthquake':
                scale = 5.96
                if title == 'map-Economic Loss by Census Tract (USD)':
                    x, y = 327 * scale, 280 * 9.96
                if title == 'map-Peak Ground Acceleration (g)':
                    x, y = 327 * scale, 127 * scale
            if self.hazard == 'hurricane':
                scale = 8.96
                if title == 'map-Economic Loss by Census Tract (USD)':
                    x, y = 327 * scale, 430 * scale
            if self.hazard == 'tsunami':
                scale = 8.96
                if title == 'map-Economic Loss by Census Block (USD)':
                    x, y = 400 * scale, 430 * scale
                if title == 'map-Travel Time to Safety (minutes)':
                    x, y = 400 * scale, 127 * scale
            try:
                self.convertPngToPdf(src, title, x, y, scale) # Added - BC
            except Exception as e:
                print('\n')
                print(e)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(fname)
                print(exc_type, exc_tb.tb_lineno)
                print('\n')
            self.convertPngToPdf(src, title, x, y, scale)  # Added - BC

        except Exception as e:
            print('\n')
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(fname)
            print(exc_type, exc_tb.tb_lineno)
            print('\n')
            raise

    def addHistogram(self, df, xCol, yCols, title, ylabel, column, colors=['#549534', '#f3de2c', '#bf2f37']):
        """ Adds a map to the report

        Keyword Arguments: \n
            df: pandas dataframe -- a geodataframe containing the data to be plotted
            xCol: str -- the categorical field
            yCols: list<str> -- the value fields
            title: str -- title for the report section
            ylabel: str -- y-axis label for the units being plotted in the yCols
            column: str -- which column in the report to add to (options: 'left', 'right')
            colors (optional if len(yCols) == 3): list<str> -- the colors for each field in yCols - should be same length (default = ['#549534', '#f3de2c', '#bf2f37'])
        """
        try:
            x = [x for x in df[xCol].values] * len(yCols)
            y = []
            hue = []
            for valueColumn in yCols:
                for category in [x for x in df[xCol].values]:
                    y.append(df[df[xCol] == category][valueColumn].values[0])
                    hue.append(valueColumn)
            dfPlot = pd.DataFrame({'x': x, 'y': y, 'hue': hue})
            plt.figure(figsize=(5, 3))
            colorPalette = dict(zip(dfPlot.hue.unique(), colors))
            ax = sns.barplot(x='x', y='y', hue='hue',
                             data=dfPlot, palette=colorPalette)
            ax.set_xlabel('')
            plt.box(on=None)
            plt.legend(title='', fontsize=8)
            plt.xticks(fontsize=8)
            plt.yticks(fontsize=8)
            fmt = '{x:,.0f}'
            tick = ticker.StrMethodFormatter(fmt)
            ax.yaxis.set_major_formatter(tick)
            plt.ylabel(ylabel, fontsize=9)
            plt.tight_layout(pad=0.1, h_pad=None, w_pad=None, rect=None)
            if not os.path.isdir(os.getcwd() + '/' + self._tempDirectory):
                os.mkdir(os.getcwd() + '/' + self._tempDirectory)
            src = os.getcwd() + '/' + self._tempDirectory + '/'+str(uuid())+".png"
            plt.savefig(src, pad_inches=0, bbox_inches='tight', dpi=600)
            plt.clf()

            template = """
                <div class="result_container">
                    <table class="results_table">
                    <tr class="results_table_header">
                        <th class="results_table_header_title_solo">
                        """+title+"""
                        </th>
                    </tr>
                    </table>
                    <img
                    class="results_table_img"
                    src='"""+src+"""'
                    alt='"""+title+"""'
                    />
                </div>
                <div class="result_container_spacer">_</div>
                """
            if column == 'left':
                self.columnLeft = self.columnLeft + template
            if column == 'right':
                self.columnRight = self.columnRight + template
              # Convert PNG to PDF
            if self.hazard == 'flood':
                scale = 13.5
                if title == 'Building Damage By Occupancy':
                    x1, y1 = 40 * scale, 550 * scale
                if title == 'Building Damage By Type':
                    x1, y1 = 40 * scale, 220 * scale
            if self.hazard == 'earthquake':
                scale = 13.5
                if title == 'Building Damage By Occupancy':
                    x1, y1 = 40 * scale, 550 * scale
            if self.hazard == 'hurricane':
                scale = 14.0
                if title == 'Building Damage By Occupancy':
                    x1, y1 = 40 * scale, 550 * scale
                if title == 'Damaged Essential Facilities':
                    x1, y1 = 40 * scale, 300 * scale
            if self.hazard == 'tsunami':
             #   scale = 17.0
                if title == 'Building Damage By Occupancy':
                    scale = 15
                    x1, y1 = 40 * scale, 560 * scale
                # if title == 'Building Damage By Type':
                #     x1, y1 = 40 * scale, 220 * scale
            if self.hazard != 'earthquake':
                self.convertPngToPdf(src, title, x1, y1, scale) # Added - BC

        except Exception as e:
            print('\n')
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(fname)
            print(exc_type, exc_tb.tb_lineno)
            print('\n')
            plt.clf()
            raise

    def save(self, path, deleteTemp=True, openFile=False, premade=None):
        """Creates a PDF of the report

        Keyword Arguments: \n
            path: str -- the output directory and file name (example: 'C://output_directory/filename.pdf')
            deleteTemp (optional): bool -- delete temp files used to create the report (default: True)
            openFile (optional): bool -- open the PDF after saving (default: True)
            premade (optional): str -- create a premade report (default: None; options: 'earthquake', 'flood', 'hurricane', 'tsunami')
        """
        try:
            if premade != None:
                try:
                    self.buildPremade(path)

                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')

            # open output file for writing (truncated binary)
            self.updateTemplate()
            result_file = open(path, "w+b")

            # convert HTML to PDF
            pisa_status = pisa.CreatePDF(
                self.template,
                dest=result_file)

            # close output file
            result_file.close()

            if openFile:
                os.startfile(path)
            if deleteTemp:
                shutil.rmtree(os.getcwd() + '/' + self._tempDirectory)
            self.columnLeft = ''
            self.columnRight = ''

            # return False on success and True on errors
            return pisa_status.err
        except Exception as e:
            print('\n')
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(fname)
            print(exc_type, exc_tb.tb_lineno)
            print('\n')
            if premade != None:
                self.columnLeft = ''
                self.columnRight = ''
            if deleteTemp:
                shutil.rmtree(os.getcwd() + '/' + self._tempDirectory)
            raise

    def buildPremade(self, path):
        """ Builds a premade report

        """
        try:
            # assign constants
            tableRowLimit = 7
            tonsToTruckLoadsCoef = 0.25
            hazard = self.hazard


###################################################
        # Earthquake
###################################################
            if hazard == 'earthquake':
                eqDataDictionary = {}
                eqDataDictionary['title'] =  self.title
                eqDataDictionary['date'] = 'Hazus Run: {}'.format(datetime.datetime.now().strftime('%m-%d-%Y').lstrip('0'))
                # get bulk of results
                try:
                    results = self._Report__getResults()
                    results = results.addGeometry()
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    ###################################
            # Earthquake - Building Damage
    ###################################
                # add building damage by occupancy
                try:
                    buildingDamageByOccupancy = self._Report__getBuildingDamageByOccupancy()

                    RES = buildingDamageByOccupancy[buildingDamageByOccupancy['Occupancy'].apply(lambda x: x.startswith('RES'))]
                    COM = buildingDamageByOccupancy[buildingDamageByOccupancy['Occupancy'].apply(lambda x: x.startswith('COM'))]
                    IND = buildingDamageByOccupancy[buildingDamageByOccupancy['Occupancy'].apply(lambda x: x.startswith('IND'))]
                    AGR = buildingDamageByOccupancy[buildingDamageByOccupancy['Occupancy'].apply(lambda x: x.startswith('AGR'))]
                    EDU = buildingDamageByOccupancy[buildingDamageByOccupancy['Occupancy'].apply(lambda x: x.startswith('EDU'))]
                    GOV = buildingDamageByOccupancy[buildingDamageByOccupancy['Occupancy'].apply(lambda x: x.startswith('GOV'))]
                    REL = buildingDamageByOccupancy[buildingDamageByOccupancy['Occupancy'].apply(lambda x: x.startswith('REL'))]
                    
                    eqDataDictionary['g_res'] = self.addCommas(RES['Minor'].sum(), abbreviate=True)
                    eqDataDictionary['g_com'] = self.addCommas(COM['Minor'].sum(), abbreviate=True)
                    eqDataDictionary['g_ind'] = self.addCommas(IND['Minor'].sum(),abbreviate=True)
                    eqDataDictionary['g_agr'] = self.addCommas(AGR['Minor'].sum(), abbreviate=True)
                    eqDataDictionary['g_edu'] = self.addCommas(EDU['Minor'].sum(), abbreviate=True)
                    eqDataDictionary['g_gov'] = self.addCommas(GOV['Minor'].sum(), abbreviate=True)
                    eqDataDictionary['g_rel'] = self.addCommas(REL['Minor'].sum(), abbreviate=True)
                    eqDataDictionary['y_res'] = self.addCommas(RES['Major'].sum(), abbreviate=True)
                    eqDataDictionary['y_com'] = self.addCommas(COM['Major'].sum(), abbreviate=True)
                    eqDataDictionary['y_ind'] = self.addCommas(IND['Major'].sum(), abbreviate=True)
                    eqDataDictionary['y_agr'] = self.addCommas(AGR['Major'].sum(), abbreviate=True)
                    eqDataDictionary['y_edu'] = self.addCommas(EDU['Major'].sum(), abbreviate=True)
                    eqDataDictionary['y_gov'] = self.addCommas(GOV['Major'].sum(), abbreviate=True)
                    eqDataDictionary['y_rel'] = self.addCommas(REL['Major'].sum(), abbreviate=True)
                    eqDataDictionary['r_res'] = self.addCommas(RES['Destroyed'].sum(), abbreviate=True)
                    eqDataDictionary['r_com'] = self.addCommas(COM['Destroyed'].sum(), abbreviate=True)
                    eqDataDictionary['r_ind'] = self.addCommas(IND['Destroyed'].sum(), abbreviate=True)
                    eqDataDictionary['r_agr'] = self.addCommas(AGR['Destroyed'].sum(), abbreviate=True)
                    eqDataDictionary['r_edu'] = self.addCommas(EDU['Destroyed'].sum(), abbreviate=True)
                    eqDataDictionary['r_gov'] = self.addCommas(GOV['Destroyed'].sum(), abbreviate=True)
                    eqDataDictionary['r_rel'] = self.addCommas(REL['Destroyed'].sum(), abbreviate=True)
                    # create category column
                    buildingDamageByOccupancy['xCol'] = [
                        x[0:3] for x in buildingDamageByOccupancy['Occupancy']]
                    # create new columns for major and destroyed
                    buildingDamageByOccupancy['Major & Destroyed'] = buildingDamageByOccupancy['Major'] + \
                        buildingDamageByOccupancy['Destroyed']
                    # list columns to group for each category
                    yCols = ['Affected', 'Minor', 'Major & Destroyed']
                    self.addHistogram(buildingDamageByOccupancy, 'xCol', yCols,
                                      'Building Damage By Occupancy', 'Buildings', 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    ###################################
            # Earthquake Economic Loss
    ###################################
                # add economic loss
                try:
                    counties = self.getCounties()
                    economicResults = results[['tract', 'EconLoss']]
                    economicResults['countyfips'] = economicResults.tract.str[:5]
                    economicLoss = pd.merge(economicResults, counties, how="inner", on=['countyfips'])
                    economicLoss.drop(['size', 'tract', 'countyfips', 'geometry', 'crs'], axis=1, inplace=True)
                    economicLoss.columns = ['EconLoss', 'Top Counties', 'State']
                    # populate total
                    total = self.addCommas(economicLoss['EconLoss'].sum(), truncate=True, abbreviate=True)
 #                   # group by county --> df.groupby(['Fruit','Name']).sum()
 #                   economicLoss = economicLoss.groupby(['Top Counties', 'State'])['EconLoss'].sum().reset_index()
                    # limit rows to the highest values
                    economicLoss = economicLoss.sort_values('EconLoss', ascending=False)[0:tableRowLimit]
                    # format values
                    economicLoss['EconLoss'] = [self.toDollars(
                        x, abbreviate=True) for x in economicLoss['EconLoss']]
                  # TODO: Use for fillable PDF & add to dictionary - create a function for this - BC
                    eqDataDictionary['econloss_county_1'] =  economicLoss['Top Counties'].iloc[0]
                    eqDataDictionary['econloss_county_2'] =  economicLoss['Top Counties'].iloc[1]
                    eqDataDictionary['econloss_county_3'] =  economicLoss['Top Counties'].iloc[2]
                    eqDataDictionary['econloss_county_4'] =  economicLoss['Top Counties'].iloc[3]
                    eqDataDictionary['econloss_county_5'] =  economicLoss['Top Counties'].iloc[4]
                    eqDataDictionary['econloss_county_6'] =  economicLoss['Top Counties'].iloc[5]
                    eqDataDictionary['econloss_county_7'] =  economicLoss['Top Counties'].iloc[6]
                    eqDataDictionary['econloss_state_1'] =  economicLoss['State'].iloc[0]
                    eqDataDictionary['econloss_state_2'] =  economicLoss['State'].iloc[1]
                    eqDataDictionary['econloss_state_3'] =  economicLoss['State'].iloc[2]
                    eqDataDictionary['econloss_state_4'] =  economicLoss['State'].iloc[3]
                    eqDataDictionary['econloss_state_5'] =  economicLoss['State'].iloc[4]
                    eqDataDictionary['econloss_state_6'] =  economicLoss['State'].iloc[5]
                    eqDataDictionary['econloss_state_7'] =  economicLoss['State'].iloc[6]
                    eqDataDictionary['econloss_total_1'] =  economicLoss['EconLoss'].iloc[0]
                    eqDataDictionary['econloss_total_2'] =  economicLoss['EconLoss'].iloc[1]
                    eqDataDictionary['econloss_total_3'] =  economicLoss['EconLoss'].iloc[2]
                    eqDataDictionary['econloss_total_4'] =  economicLoss['EconLoss'].iloc[3]
                    eqDataDictionary['econloss_total_5'] =  economicLoss['EconLoss'].iloc[4]
                    eqDataDictionary['econloss_total_6'] =  economicLoss['EconLoss'].iloc[5]
                    eqDataDictionary['econloss_total_7'] =  economicLoss['EconLoss'].iloc[6]
                    eqDataDictionary['total_econloss'] =  '$' + total
                  #  'total_econloss': total - Add to table
                    self.addTable(
                        economicLoss, 'Total Economic Loss', total, 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    ####################################################
            # Earthquake - Injuries and Fatalities
    ####################################################
                # add injuries and fatatilies
                try:
                    counties = self.getCounties()
                    injuriesAndFatatiliesResults = results[['tract']]
                    injuriesAndFatatiliesResults['countyfips'] = injuriesAndFatatiliesResults.tract.str[:5]
                    injuriesAndFatatilies = pd.merge(injuriesAndFatatiliesResults, counties, how="inner", on=['countyfips'])
                    injuriesAndFatatilies.drop(['size', 'countyfips', 'geometry', 'crs', 'tract'], axis=1, inplace=True)
                    injuriesAndFatatilies.columns = ['Top Counties', 'State']
                    injuriesAndFatatilies['Injuries Day'] = results['Injury_DayLevel1'] + \
                        results['Injury_DayLevel2'] + \
                        results['Injury_DayLevel3']
                    injuriesAndFatatilies['Injuries Night'] = results['Injury_NightLevel1'] + \
                        results['Injury_NightLevel2'] + \
                        results['Injury_NightLevel3']
                    injuriesAndFatatilies['Fatalities Day'] = results['Fatalities_Day']
                    injuriesAndFatatilies['Fatalities Night'] = results['Fatalities_Night']
                    # populate totals
                    totalDay = self.addCommas(
                        (injuriesAndFatatilies['Injuries Day'] + injuriesAndFatatilies['Fatalities Day']).sum(), abbreviate=True) + ' Day'
                    totalNight = self.addCommas(
                        (injuriesAndFatatilies['Injuries Night'] + injuriesAndFatatilies['Fatalities Night']).sum(), abbreviate=True) + ' Night'
                    total = totalDay + '/' + totalNight
                    # limit rows to the highest values
                    injuriesAndFatatilies = injuriesAndFatatilies.sort_values(
                        'Injuries Day', ascending=False)[0:tableRowLimit]
                    # format values
                    for column in injuriesAndFatatilies:
                        if column not in ['Top Counties', 'State']:
                            injuriesAndFatatilies[column] = [self.addCommas(
                                x, abbreviate=True) for x in injuriesAndFatatilies[column]]
                    eqDataDictionary['nonfatal_county_1'] =  injuriesAndFatatilies['Top Counties'].iloc[0]
                    eqDataDictionary['nonfatal_county_2'] =  injuriesAndFatatilies['Top Counties'].iloc[1]
                    eqDataDictionary['nonfatal_county_3'] =  injuriesAndFatatilies['Top Counties'].iloc[2]
                    eqDataDictionary['nonfatal_county_4'] =  injuriesAndFatatilies['Top Counties'].iloc[3]
                    eqDataDictionary['nonfatal_county_5'] =  injuriesAndFatatilies['Top Counties'].iloc[4]
                    eqDataDictionary['nonfatal_county_6'] =  injuriesAndFatatilies['Top Counties'].iloc[5]
                    eqDataDictionary['nonfatal_county_7'] =  injuriesAndFatatilies['Top Counties'].iloc[6]
                    eqDataDictionary['nonfatal_state_1'] =  injuriesAndFatatilies['State'].iloc[0]
                    eqDataDictionary['nonfatal_state_2'] =  injuriesAndFatatilies['State'].iloc[1]
                    eqDataDictionary['nonfatal_state_3'] =  injuriesAndFatatilies['State'].iloc[2]
                    eqDataDictionary['nonfatal_state_4'] =  injuriesAndFatatilies['State'].iloc[3]
                    eqDataDictionary['nonfatal_state_5'] =  injuriesAndFatatilies['State'].iloc[4]
                    eqDataDictionary['nonfatal_state_6'] =  injuriesAndFatatilies['State'].iloc[5]
                    eqDataDictionary['nonfatal_state_7'] =  injuriesAndFatatilies['State'].iloc[6]
                    eqDataDictionary['nonfatal_pop_1'] =  injuriesAndFatatilies['Injuries Day'].iloc[0] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[0]
                    eqDataDictionary['nonfatal_pop_2'] =  injuriesAndFatatilies['Injuries Day'].iloc[1] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[1]
                    eqDataDictionary['nonfatal_pop_3'] =  injuriesAndFatatilies['Injuries Day'].iloc[2] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[2]
                    eqDataDictionary['nonfatal_pop_4'] =  injuriesAndFatatilies['Injuries Day'].iloc[3] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[3]
                    eqDataDictionary['nonfatal_pop_5'] =  injuriesAndFatatilies['Injuries Day'].iloc[4] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[4]
                    eqDataDictionary['nonfatal_pop_6'] =  injuriesAndFatatilies['Injuries Day'].iloc[5] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[5]
                    eqDataDictionary['nonfatal_pop_7'] =  injuriesAndFatatilies['Injuries Day'].iloc[6] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[6]
                    eqDataDictionary['nonfatal_injuries_1'] =  injuriesAndFatatilies['Fatalities Day'].iloc[0]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[0]
                    eqDataDictionary['nonfatal_injuries_2'] =  injuriesAndFatatilies['Fatalities Day'].iloc[1]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[1]
                    eqDataDictionary['nonfatal_injuries_3'] =  injuriesAndFatatilies['Fatalities Day'].iloc[2]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[2]
                    eqDataDictionary['nonfatal_injuries_4'] =  injuriesAndFatatilies['Fatalities Day'].iloc[3]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[3]
                    eqDataDictionary['nonfatal_injuries_5'] =  injuriesAndFatatilies['Fatalities Day'].iloc[4]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[4]
                    eqDataDictionary['nonfatal_injuries_6'] =  injuriesAndFatatilies['Fatalities Day'].iloc[5]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[5]
                    eqDataDictionary['nonfatal_injuries_7'] =  injuriesAndFatatilies['Fatalities Day'].iloc[6]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[6]
                    eqDataDictionary['total_injuries'] =  total
                    self.addTable(injuriesAndFatatilies, 'Injuries and Fatatilies', total, 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    ################################################
            # Earthquake - Displaced Households & Shelter Needs
    ################################################
                # add displaced households and shelter needs
                try:
                    counties = self.getCounties()
                    displacedAndShelterResults = results[['tract', 'DisplacedHouseholds', 'ShelterNeeds']]
                    displacedAndShelterResults['countyfips'] = displacedAndShelterResults.tract.str[:5]
                    displacedAndShelter = pd.merge(displacedAndShelterResults, counties, how="inner", on=['countyfips'])
                    displacedAndShelter.drop(['size', 'countyfips', 'geometry', 'crs', 'tract'], axis=1, inplace=True)
                    displacedAndShelter.columns = [
                         'DisplacedHouseholds', 'ShelterNeeds', 'Top Counties', 'State']
                    # populate totals
                    totalDisplaced = self.addCommas(
                        displacedAndShelter['DisplacedHouseholds'].sum(), abbreviate=True)
                    totalShelter = self.addCommas(
                        displacedAndShelter['ShelterNeeds'].sum(), abbreviate=True)
                    total = totalDisplaced + ' Displaced/' + totalShelter + ' Needing Shelter'
                    # limit rows to the highest values
                    displacedAndShelter = displacedAndShelter.sort_values(
                        'DisplacedHouseholds', ascending=False)[0:tableRowLimit]
                    # format values
                    for column in displacedAndShelter:
                        if column != 'Top Census Tracts':
                            displacedAndShelter[column] = [self.addCommas(
                                x, abbreviate=True) for x in displacedAndShelter[column]]
                    eqDataDictionary['shelter_county_1'] =  displacedAndShelter['Top Counties'].iloc[0]
                    eqDataDictionary['shelter_county_2'] =  displacedAndShelter['Top Counties'].iloc[1]
                    eqDataDictionary['shelter_county_3'] =  displacedAndShelter['Top Counties'].iloc[2]
                    eqDataDictionary['shelter_county_4'] =  displacedAndShelter['Top Counties'].iloc[3]
                    eqDataDictionary['shelter_county_5'] =  displacedAndShelter['Top Counties'].iloc[4]
                    eqDataDictionary['shelter_county_6'] =  displacedAndShelter['Top Counties'].iloc[5]
                    eqDataDictionary['shelter_county_7'] =  displacedAndShelter['Top Counties'].iloc[6]
                    eqDataDictionary['shelter_state_1'] =  displacedAndShelter['State'].iloc[0]
                    eqDataDictionary['shelter_state_2'] =  displacedAndShelter['State'].iloc[1]
                    eqDataDictionary['shelter_state_3'] =  displacedAndShelter['State'].iloc[2]
                    eqDataDictionary['shelter_state_4'] =  displacedAndShelter['State'].iloc[3]
                    eqDataDictionary['shelter_state_5'] =  displacedAndShelter['State'].iloc[4]
                    eqDataDictionary['shelter_state_6'] =  displacedAndShelter['State'].iloc[5]
                    eqDataDictionary['shelter_state_7'] =  displacedAndShelter['State'].iloc[6]
                    eqDataDictionary['shelter_house_1'] =  displacedAndShelter['DisplacedHouseholds'].iloc[0]
                    eqDataDictionary['shelter_house_2'] =  displacedAndShelter['DisplacedHouseholds'].iloc[1]
                    eqDataDictionary['shelter_house_3'] =  displacedAndShelter['DisplacedHouseholds'].iloc[2]
                    eqDataDictionary['shelter_house_4'] =  displacedAndShelter['DisplacedHouseholds'].iloc[3]
                    eqDataDictionary['shelter_house_5'] =  displacedAndShelter['DisplacedHouseholds'].iloc[4]
                    eqDataDictionary['shelter_house_6'] =  displacedAndShelter['DisplacedHouseholds'].iloc[5]
                    eqDataDictionary['shelter_house_7'] =  displacedAndShelter['DisplacedHouseholds'].iloc[6]
                    eqDataDictionary['shelter_need_1'] =  displacedAndShelter['ShelterNeeds'].iloc[0]
                    eqDataDictionary['shelter_need_2'] =  displacedAndShelter['ShelterNeeds'].iloc[1]
                    eqDataDictionary['shelter_need_3'] =  displacedAndShelter['ShelterNeeds'].iloc[2]
                    eqDataDictionary['shelter_need_4'] =  displacedAndShelter['ShelterNeeds'].iloc[3]
                    eqDataDictionary['shelter_need_5'] =  displacedAndShelter['ShelterNeeds'].iloc[4]
                    eqDataDictionary['shelter_need_6'] =  displacedAndShelter['ShelterNeeds'].iloc[5]
                    eqDataDictionary['shelter_need_7'] =  displacedAndShelter['ShelterNeeds'].iloc[6]
                    eqDataDictionary['total_shelter'] =  total
                    self.addTable(
                        displacedAndShelter, 'Displaced Households and Short-Term Shelter Needs', total, 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    ###################################
            # Earthquake - Economic Loss Map
    ###################################
                # add economic loss map
                try:
                    economicLoss = results[['tract', 'EconLoss', 'geometry']]
                    breaks = nb(results['EconLoss'], nb_class=5)
                    legend_item1 = breaks[0]
                    legend_item2 = breaks[1]
                    legend_item3 = breaks[2]
                    legend_item4 = breaks[3]
                    legend_item5 = breaks[4]
                    eqDataDictionary['legend_1'] =  '$' + self.abbreviate(legend_item1) + '-' + self.abbreviate(legend_item2)
                    eqDataDictionary['legend_2'] =  '$' + self.abbreviate(legend_item2) + '-' + self.abbreviate(legend_item3)
                    eqDataDictionary['legend_3'] =  '$' + self.abbreviate(legend_item3) + '-' + self.abbreviate(legend_item4)
                    eqDataDictionary['legend_4'] =  '$' + self.abbreviate(legend_item4) + '-' + self.abbreviate(legend_item5)
                    # convert to GeoDataFrame
                    economicLoss.geometry = economicLoss.geometry.apply(loads)
                    gdf = gpd.GeoDataFrame(economicLoss)
                    self.addMap(gdf, title='Economic Loss by Census Tract (USD)',
                                column='right', field='EconLoss', cmap='OrRd')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    ###################################
            # Earthquake - Hazard Map
    ###################################
                # add hazard map
                try:
                    gdf = self._Report__getHazardGeoDataFrame()
                    title = gdf.title
                    # limit the extent
                    gdf = gdf[gdf['PARAMVALUE'] > 0.1]
                    self.addMap(gdf, title=title,
                                column='right', field='PARAMVALUE', formatTicks=False, cmap='coolwarm')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    ###################################
            # Earthquake - Add Debris
    ###################################
                # add debris
                try:
                    # populate and format values
                    bwTons = self.addCommas(
                        results['DebrisBW'].sum(), abbreviate=True)
                    csTons = self.addCommas(
                        results['DebrisCS'].sum(), abbreviate=True)
                    bwTruckLoads = self.addCommas(
                        results['DebrisBW'].sum() * tonsToTruckLoadsCoef, abbreviate=True)
                    csTruckLoads = self.addCommas(
                        results['DebrisCS'].sum() * tonsToTruckLoadsCoef, abbreviate=True)
                    # populate totals
                    totalTons = self.addCommas(
                        results['DebrisTotal'].sum(), abbreviate=True)
                    totalTruckLoads = self.addCommas(
                        results['DebrisTotal'].sum() * tonsToTruckLoadsCoef, abbreviate=True)
                    total = totalTons + ' Tons/' + totalTruckLoads + ' Truck Loads'
                    # build data dictionary
                    data = {'Debris Type': ['Brick, Wood, and Others', 'Concrete & Steel'], 'Tons': [
                        bwTons, csTons], 'Truck Loads': [bwTruckLoads, csTruckLoads]}
                    # create DataFrame from data dictionary
                    debris = pd.DataFrame(
                        data, columns=['Debris Type', 'Tons', 'Truck Loads'])
                    eqDataDictionary['debris_type_1'] =  debris['Debris Type'][0]
                    eqDataDictionary['debris_tons_1'] = debris['Tons'][0]              #TODO: Check if we want truck loads amount - BC
                    eqDataDictionary['debris_type_2'] = debris['Debris Type'][1]
                    eqDataDictionary['debris_tons_2'] = debris['Tons'][1]
                    eqDataDictionary['total_debris']  =  total
                    self.addTable(debris, 'Debris', total, 'right')
                    self.mergePdfs()        # Added - BC
                    self.mergeToTemplate()  # Added - BC
                    self.writeFillablePdf(eqDataDictionary, path) # Added - BC
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

###################################################
        # Flood
###################################################
            if hazard == 'flood':
                floodDataDictionary = {}
                floodDataDictionary['title'] =  self.title
                floodDataDictionary['date'] = 'Hazus Run: {}'.format(datetime.datetime.now().strftime('%m-%d-%Y').lstrip('0'))
                # get bulk of results
                try:
                    results = self._Report__getResults()
                    results = results.addGeometry()
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    ###################################
            # Flood - Building Damage
    ###################################
                try:
                    # add Building Damage by occupancy
                    buildingDamageByOccupancy = self._Report__getBuildingDamageByOccupancy()
                    # reorder the columns
                    cols = buildingDamageByOccupancy.columns.tolist()
                    cols = [cols[0]] + cols[2:] + [cols[1]]
                    buildingDamageByOccupancy = buildingDamageByOccupancy[cols]
                    # list columns to group for each category
                    yCols = ['Building Loss', 'Content Loss', 'Total Loss']
                    # rename the columns
                    buildingDamageByOccupancy.columns = ['Occupancy'] + yCols
                    # floodDataDictionary['Occupancy Building Loss'] =  self.title
                    # floodDataDictionary['Occupancy '] =  self.title
                    # floodDataDictionary['Occupancy'] =  self.title
                    # create category column
                    buildingDamageByOccupancy['xCol'] = [
                        x[0:3] for x in buildingDamageByOccupancy['Occupancy']]
                    self.addHistogram(buildingDamageByOccupancy, 'xCol', yCols,
                                      'Building Damage By Occupancy', 'Buildings', 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    ###################################
            # Flood Economic Loss
    ###################################
                # add economic loss
                try:
                    # TODO: Add county to dataframe -BC
                    counties = self.getCounties()
                    economicResults = results[['block', 'EconLoss']]
                    economicResults['countyfips'] = economicResults.block.str[:5]
                    economicLoss = pd.merge(economicResults, counties, how="inner", on=['countyfips'])
                    economicLoss.drop(['size', 'countyfips', 'state', 'geometry', 'crs'], axis=1, inplace=True)
                    economicLoss.columns = ['Top Census Blocks', 'Economic Loss', 'CountyName']
                    # populate total
                    total = self.addCommas(
                        economicLoss['Economic Loss'].sum(), truncate=True, abbreviate=True)
                    # limit rows to the highest values
                    print('Total Economic Loss: {}'.format(total))
                    economicLoss = economicLoss.sort_values(
                        'Economic Loss', ascending=False)[0:tableRowLimit]
                    # format values
                    economicLoss['Economic Loss'] = [self.toDollars(
                        x, abbreviate=True) for x in economicLoss['Economic Loss']]
                    # TODO: Use for fillable PDF & add to dictionary - create a function for this - BC
                    floodDataDictionary['econloss_county_1'] =  economicLoss['Top Census Blocks'].iloc[0]
                    floodDataDictionary['econloss_county_2'] =  economicLoss['Top Census Blocks'].iloc[1]
                    floodDataDictionary['econloss_county_3'] =  economicLoss['Top Census Blocks'].iloc[2]
                    floodDataDictionary['econloss_county_4'] =  economicLoss['Top Census Blocks'].iloc[3]
                    floodDataDictionary['econloss_county_5'] =  economicLoss['Top Census Blocks'].iloc[4]
                    floodDataDictionary['econloss_county_6'] =  economicLoss['Top Census Blocks'].iloc[5]
                    floodDataDictionary['econloss_county_7'] =  economicLoss['Top Census Blocks'].iloc[6]
                    floodDataDictionary['econloss_state_1'] =  economicLoss['CountyName'].iloc[0]
                    floodDataDictionary['econloss_state_2'] =  economicLoss['CountyName'].iloc[1]
                    floodDataDictionary['econloss_state_3'] =  economicLoss['CountyName'].iloc[2]
                    floodDataDictionary['econloss_state_4'] =  economicLoss['CountyName'].iloc[3]
                    floodDataDictionary['econloss_state_5'] =  economicLoss['CountyName'].iloc[4]
                    floodDataDictionary['econloss_state_6'] =  economicLoss['CountyName'].iloc[5]
                    floodDataDictionary['econloss_state_7'] =  economicLoss['CountyName'].iloc[6]
                    floodDataDictionary['econloss_total_1'] =  economicLoss['Economic Loss'].iloc[0]
                    floodDataDictionary['econloss_total_2'] =  economicLoss['Economic Loss'].iloc[1]
                    floodDataDictionary['econloss_total_3'] =  economicLoss['Economic Loss'].iloc[2]
                    floodDataDictionary['econloss_total_4'] =  economicLoss['Economic Loss'].iloc[3]
                    floodDataDictionary['econloss_total_5'] =  economicLoss['Economic Loss'].iloc[4]
                    floodDataDictionary['econloss_total_6'] =  economicLoss['Economic Loss'].iloc[5]
                    floodDataDictionary['econloss_total_7'] =  economicLoss['Economic Loss'].iloc[6]
                    floodDataDictionary['total_econloss'] =  total
                  #  'total_econloss': total - Add to table
                    self.addTable(
                        economicLoss, 'Total Economic Loss', total, 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

    #######################################################
            # Flood - Building Damaged by Building Type
    #######################################################
                # add building damage by building type
                try:
                    buildingDamageByType = self._Report__getBuildingDamageByType()
                    # reorder the columns
                    cols = buildingDamageByType.columns.tolist()
                    cols = [cols[0]] + cols[2:] + [cols[1]]
                    buildingDamageByType = buildingDamageByType[cols]
                    # list columns to group for each category
                    yCols = ['Building Loss', 'Content Loss', 'Total Loss']
                    # rename the columns & create category column
                    buildingDamageByType.columns = ['xCol'] + yCols
                    self.addHistogram(buildingDamageByType, 'xCol', yCols,
                                      'Building Damage By Type', 'Dollars (USD)', 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

    ################################################
            # Flood - Displaced Households & Shelter Needs
    ################################################
                # add displaced households and shelter needs
                try:
                    displacedAndShelter = results[[
                        'block', 'DisplacedHouseholds', 'ShelterNeeds']]
                    displacedAndShelter.columns = [
                        'Top Census Blocks', 'Displaced Households', 'People Needing Shelter']
                    # populate totals
                    totalDisplaced = self.addCommas(
                        displacedAndShelter['Displaced Households'].sum(), abbreviate=True)
                    totalShelter = self.addCommas(
                        displacedAndShelter['People Needing Shelter'].sum(), abbreviate=True)
                    total = totalDisplaced + ' Displaced/' + totalShelter + ' Needing Shelter'
                    print('Total Displaced Households: {}'.format(total))
                    # limit rows to the highest values
                    displacedAndShelter = displacedAndShelter.sort_values(
                        'Displaced Households', ascending=False)[0:tableRowLimit]
                    # format values
                    for column in displacedAndShelter:
                        if column != 'Top Census Blocks':
                            displacedAndShelter[column] = [self.addCommas(
                                x, abbreviate=True) for x in displacedAndShelter[column]]
                  # TODO: Use for fillable PDF & add to dictionary - create a function for this - BC
                    floodDataDictionary['shelter_county_1'] =  displacedAndShelter['Top Census Blocks'].iloc[0]
                    floodDataDictionary['shelter_county_2'] =  displacedAndShelter['Top Census Blocks'].iloc[1]
                    floodDataDictionary['shelter_county_3'] =  displacedAndShelter['Top Census Blocks'].iloc[2]
                    floodDataDictionary['shelter_county_4'] =  displacedAndShelter['Top Census Blocks'].iloc[3]
                    floodDataDictionary['shelter_county_5'] =  displacedAndShelter['Top Census Blocks'].iloc[4]
                    floodDataDictionary['shelter_county_6'] =  displacedAndShelter['Top Census Blocks'].iloc[5]
                    floodDataDictionary['shelter_county_7'] =  displacedAndShelter['Top Census Blocks'].iloc[6]
                    floodDataDictionary['shelter_house_1'] =  displacedAndShelter['Displaced Households'].iloc[0]
                    floodDataDictionary['shelter_house_2'] =  displacedAndShelter['Displaced Households'].iloc[1]
                    floodDataDictionary['shelter_house_3'] =  displacedAndShelter['Displaced Households'].iloc[2]
                    floodDataDictionary['shelter_house_4'] =  displacedAndShelter['Displaced Households'].iloc[3]
                    floodDataDictionary['shelter_house_5'] =  displacedAndShelter['Displaced Households'].iloc[4]
                    floodDataDictionary['shelter_house_6'] =  displacedAndShelter['Displaced Households'].iloc[5]
                    floodDataDictionary['shelter_house_7'] =  displacedAndShelter['Displaced Households'].iloc[6]
                    floodDataDictionary['shelter_need_1'] =  displacedAndShelter['People Needing Shelter'].iloc[0]
                    floodDataDictionary['shelter_need_2'] =  displacedAndShelter['People Needing Shelter'].iloc[1]
                    floodDataDictionary['shelter_need_3'] =  displacedAndShelter['People Needing Shelter'].iloc[2]
                    floodDataDictionary['shelter_need_4'] =  displacedAndShelter['People Needing Shelter'].iloc[3]
                    floodDataDictionary['shelter_need_5'] =  displacedAndShelter['People Needing Shelter'].iloc[4]
                    floodDataDictionary['shelter_need_6'] =  displacedAndShelter['People Needing Shelter'].iloc[5]
                    floodDataDictionary['shelter_need_7'] =  displacedAndShelter['People Needing Shelter'].iloc[6]
                    floodDataDictionary['total_shelter'] =  total
                    self.addTable(
                        displacedAndShelter, 'Displaced Households and Sort-Term Shelter Needs', total, 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

    ###################################
            # Flood - Economic Loss Map
    ###################################
                # add economic loss map
                try:
                    economicLoss = results[['block', 'EconLoss', 'geometry']]
                    # convert to GeoDataFrame
                    breaks = nb(results['EconLoss'], nb_class=5)
                    legend_item1 = breaks[0]
                    legend_item2 = breaks[1]
                    legend_item3 = breaks[2]
                    legend_item4 = breaks[3]
                    legend_item5 = breaks[4]
                    floodDataDictionary['legend_1'] =  '$' + self.abbreviate(legend_item1) + '-' + self.abbreviate(legend_item2)
                    floodDataDictionary['legend_2'] =  '$' + self.abbreviate(legend_item2) + '-' + self.abbreviate(legend_item3)
                    floodDataDictionary['legend_3'] =  '$' + self.abbreviate(legend_item3) + '-' + self.abbreviate(legend_item4)
                    floodDataDictionary['legend_4'] =  '$' + self.abbreviate(legend_item4) + '-' + self.abbreviate(legend_item5)
                    economicLoss.geometry = economicLoss.geometry.apply(loads)
                    gdf = gpd.GeoDataFrame(economicLoss)
                    self.addMap(gdf, title='Economic Loss by Census Block',
                                column='right', field='EconLoss', cmap='OrRd')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

    ###################################
            # Flood - Hazard Map
    ###################################
                # add hazard map
                try:
                    gdf = self._Report__getHazardGeoDataFrame()
                    title = gdf.title
                    self.addMap(gdf, title=title,
                                column='right', field='PARAMVALUE', formatTicks=False, cmap='Blues')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

    ###################################
            # Flood - Add Debris
    ###################################
                # add debris
                try:
                    # populate and format values
                    tons = self.addCommas(
                        results['DebrisTotal'].sum(), abbreviate=True)
                    truckLoads = self.addCommas(
                        results['DebrisTotal'].sum() * tonsToTruckLoadsCoef, abbreviate=True)
                    # populate totals
                    totalTons = tons
                    totalTruckLoads = truckLoads
                    total = totalTons + ' Tons/' + totalTruckLoads + ' Truck Loads'

                    # build data dictionary
                    data = {'Debris Type': ['All Debris'], 'Tons': [
                        tons], 'Truck Loads': [truckLoads]}
                    # create DataFrame from data dictionary
                    debris = pd.DataFrame(
                        data, columns=['Debris Type', 'Tons', 'Truck Loads'])
                    floodDataDictionary['debris_type_1'] =  debris['Debris Type'][0]
                    floodDataDictionary['debris_tons_1'] =  totalTons                   #TODO: Check if we want truck loads amount - BC
                    floodDataDictionary['total_debris'] =  total
                    self.addTable(debris, 'Debris', total, 'right')
                    self.mergePdfs()        # Added - BC
                    self.mergeToTemplate()  # Added - BC
                    self.writeFillablePdf(floodDataDictionary, path) # Added - BC
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

###################################################
        # Hurricane
###################################################
            if hazard == 'hurricane':
                # get bulk of results
                hurDataDictionary = {}
                hurDataDictionary['title'] =  self.title
                hurDataDictionary['date'] = 'Hazus Run: {}'.format(datetime.datetime.now().strftime('%m-%d-%Y').lstrip('0'))
                try:
                    results = self._Report__getResults()
                    results = results.addGeometry()
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    #######################################
            # Hurricane - Building Damage
    #######################################
                # add building damage by occupancy
                try:
                    buildingDamageByOccupancy = self._Report__getBuildingDamageByOccupancy()
                    # create category column
                    buildingDamageByOccupancy['xCol'] = [
                        x[0:3] for x in buildingDamageByOccupancy['Occupancy']]
                    # create new columns for major and destroyed
                    buildingDamageByOccupancy['Major & Destroyed'] = buildingDamageByOccupancy['Major'] + \
                        buildingDamageByOccupancy['Destroyed']
                    # list columns to group for each category
                    yCols = ['Affected', 'Minor', 'Major & Destroyed']
                    self.addHistogram(buildingDamageByOccupancy, 'xCol', yCols,
                                      'Building Damage By Occupancy', 'Buildings', 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    ###################################
            # Hurricane Economic Loss
    ###################################
                # add economic loss
                try:
                    counties = self.getCounties()
                    economicResults = results[['tract', 'EconLoss']]
                    economicResults['countyfips'] = economicResults.tract.str[:5]
                    economicLoss = pd.merge(economicResults, counties, how="inner", on=['countyfips'])
                    economicLoss.drop(['size', 'countyfips', 'tract', 'geometry', 'crs'], axis=1, inplace=True)
                    economicLoss.columns = [
                        'Economic Loss', 'County Name', 'State']
                    # populate total
                    total = self.addCommas(
                        economicLoss['Economic Loss'].sum(), truncate=True, abbreviate=True)
                    # limit rows to the highest values
                    economicLoss = economicLoss.sort_values(
                        'Economic Loss', ascending=False)[0:tableRowLimit]
                    # format values
                    economicLoss['Economic Loss'] = [self.toDollars(
                        x, abbreviate=True) for x in economicLoss['Economic Loss']]
                    hurDataDictionary['econloss_county_1'] =  economicLoss['County Name'].iloc[0]
                    hurDataDictionary['econloss_county_2'] =  economicLoss['County Name'].iloc[1]
                    hurDataDictionary['econloss_county_3'] =  economicLoss['County Name'].iloc[2]
                    hurDataDictionary['econloss_county_4'] =  economicLoss['County Name'].iloc[3]
                    hurDataDictionary['econloss_county_5'] =  economicLoss['County Name'].iloc[4]
                    hurDataDictionary['econloss_county_6'] =  economicLoss['County Name'].iloc[5]
                    hurDataDictionary['econloss_county_7'] =  economicLoss['County Name'].iloc[6]
                    hurDataDictionary['econloss_state_1'] =  economicLoss['State'].iloc[0]
                    hurDataDictionary['econloss_state_2'] =  economicLoss['State'].iloc[1]
                    hurDataDictionary['econloss_state_3'] =  economicLoss['State'].iloc[2]
                    hurDataDictionary['econloss_state_4'] =  economicLoss['State'].iloc[3]
                    hurDataDictionary['econloss_state_5'] =  economicLoss['State'].iloc[4]
                    hurDataDictionary['econloss_state_6'] =  economicLoss['State'].iloc[5]
                    hurDataDictionary['econloss_state_7'] =  economicLoss['State'].iloc[6]
                    hurDataDictionary['econloss_total_1'] =  economicLoss['Economic Loss'].iloc[0]
                    hurDataDictionary['econloss_total_2'] =  economicLoss['Economic Loss'].iloc[1]
                    hurDataDictionary['econloss_total_3'] =  economicLoss['Economic Loss'].iloc[2]
                    hurDataDictionary['econloss_total_4'] =  economicLoss['Economic Loss'].iloc[3]
                    hurDataDictionary['econloss_total_5'] =  economicLoss['Economic Loss'].iloc[4]
                    hurDataDictionary['econloss_total_6'] =  economicLoss['Economic Loss'].iloc[5]
                    hurDataDictionary['econloss_total_7'] =  economicLoss['Economic Loss'].iloc[6]
                    hurDataDictionary['total_econloss'] =  total
                    self.addTable(
                        economicLoss, 'Total Economic Loss', total, 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    #######################################################
            # Hurricane - Damaged Essential Facilities
    #######################################################
                # add essential facilities
                try:
                    essentialFacilities = self._Report__getEssentialFacilities()
                    print('\nThese are Essential Facilities:')
                    print(essentialFacilities.head(3))
                    # create category column
                    essentialFacilities.columns = [
                        x.replace('FacilityType', 'xCol') for x in essentialFacilities.columns]
                    essentialFacilities['Major & Destroyed'] = essentialFacilities['Major'] + \
                        essentialFacilities['Destroyed']
                    # list columns to group for each category
                    yCols = ['Affected', 'Minor', 'Major & Destroyed']
                    self.addHistogram(essentialFacilities, 'xCol', yCols,
                                      'Damaged Essential Facilities', 'Total Facilities', 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

    ###########################################################
            # Hurricane - Displaced Households & Shelter Needs
    ###########################################################
                # add displaced households and shelter needs
                try:
                    counties = self.getCounties()
                    displacedAndShelterResults = results[['tract', 'DisplacedHouseholds', 'ShelterNeeds']]
                    displacedAndShelterResults['countyfips'] = displacedAndShelterResults.tract.str[:5]
                    displacedAndShelter = pd.merge(displacedAndShelterResults, counties, how="inner", on=['countyfips'])
                    displacedAndShelter.drop(['size', 'countyfips', 'geometry', 'crs', 'tract'], axis=1, inplace=True)
                    displacedAndShelter.columns = [
                         'DisplacedHouseholds', 'ShelterNeeds', 'Top Counties', 'State']
                    # populate totals
                    totalDisplaced = self.addCommas(
                        displacedAndShelter['DisplacedHouseholds'].sum(), abbreviate=True)
                    totalShelter = self.addCommas(
                        displacedAndShelter['ShelterNeeds'].sum(), abbreviate=True)
                    total = totalDisplaced + ' Displaced/' + totalShelter + ' Needing Shelter'
                    # limit rows to the highest values
                    displacedAndShelter = displacedAndShelter.sort_values(
                        'DisplacedHouseholds', ascending=False)[0:tableRowLimit]
                    # format values
                    for column in displacedAndShelter:
                        if column != 'Top Census Tracts':
                            displacedAndShelter[column] = [self.addCommas(
                                x, abbreviate=True) for x in displacedAndShelter[column]]
                    hurDataDictionary['shelter_county_1'] =  displacedAndShelter['Top Counties'].iloc[0]
                    hurDataDictionary['shelter_county_2'] =  displacedAndShelter['Top Counties'].iloc[1]
                    hurDataDictionary['shelter_county_3'] =  displacedAndShelter['Top Counties'].iloc[2]
                    hurDataDictionary['shelter_county_4'] =  displacedAndShelter['Top Counties'].iloc[3]
                    hurDataDictionary['shelter_county_5'] =  displacedAndShelter['Top Counties'].iloc[4]
                    hurDataDictionary['shelter_county_6'] =  displacedAndShelter['Top Counties'].iloc[5]
                    hurDataDictionary['shelter_county_7'] =  displacedAndShelter['Top Counties'].iloc[6]
                    hurDataDictionary['shelter_state_1'] =  displacedAndShelter['State'].iloc[0]
                    hurDataDictionary['shelter_state_2'] =  displacedAndShelter['State'].iloc[1]
                    hurDataDictionary['shelter_state_3'] =  displacedAndShelter['State'].iloc[2]
                    hurDataDictionary['shelter_state_4'] =  displacedAndShelter['State'].iloc[3]
                    hurDataDictionary['shelter_state_5'] =  displacedAndShelter['State'].iloc[4]
                    hurDataDictionary['shelter_state_6'] =  displacedAndShelter['State'].iloc[5]
                    hurDataDictionary['shelter_state_7'] =  displacedAndShelter['State'].iloc[6]
                    hurDataDictionary['shelter_house_1'] =  displacedAndShelter['DisplacedHouseholds'].iloc[0]
                    hurDataDictionary['shelter_house_2'] =  displacedAndShelter['DisplacedHouseholds'].iloc[1]
                    hurDataDictionary['shelter_house_3'] =  displacedAndShelter['DisplacedHouseholds'].iloc[2]
                    hurDataDictionary['shelter_house_4'] =  displacedAndShelter['DisplacedHouseholds'].iloc[3]
                    hurDataDictionary['shelter_house_5'] =  displacedAndShelter['DisplacedHouseholds'].iloc[4]
                    hurDataDictionary['shelter_house_6'] =  displacedAndShelter['DisplacedHouseholds'].iloc[5]
                    hurDataDictionary['shelter_house_7'] =  displacedAndShelter['DisplacedHouseholds'].iloc[6]
                    hurDataDictionary['shelter_need_1'] =  displacedAndShelter['ShelterNeeds'].iloc[0]
                    hurDataDictionary['shelter_need_2'] =  displacedAndShelter['ShelterNeeds'].iloc[1]
                    hurDataDictionary['shelter_need_3'] =  displacedAndShelter['ShelterNeeds'].iloc[2]
                    hurDataDictionary['shelter_need_4'] =  displacedAndShelter['ShelterNeeds'].iloc[3]
                    hurDataDictionary['shelter_need_5'] =  displacedAndShelter['ShelterNeeds'].iloc[4]
                    hurDataDictionary['shelter_need_6'] =  displacedAndShelter['ShelterNeeds'].iloc[5]
                    hurDataDictionary['shelter_need_7'] =  displacedAndShelter['ShelterNeeds'].iloc[6]
                    hurDataDictionary['total_shelter'] =  total
                    self.addTable(
                        displacedAndShelter, 'Displaced Households and Sort-Term Shelter Needs', total, 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

    ########################################
            # Hurricane - Economic Loss Map
    ########################################
                # add economic loss map
                try:
                    economicLoss = results[['tract', 'EconLoss', 'geometry']]
                    # convert to GeoDataFrame
                    economicLoss.geometry = economicLoss.geometry.apply(loads)
                    gdf = gpd.GeoDataFrame(economicLoss)
                    self.addMap(gdf, title='Economic Loss by Census Tract (USD)', column='right', field='EconLoss', cmap='OrRd')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

    ###################################
            # Hurricane - Hazard Map
    ###################################
                # add hazard map
                try:
                    gdf = self._Report__getHazardGeoDataFrame()
                    title = gdf.title
                    # limit the extent
                    gdf = gdf[gdf['PARAMVALUE'] > 0.1]
                    self.addMap(gdf, title=title, column='right', field='PARAMVALUE', formatTicks=False, cmap='coolwarm')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    ###################################
            # Hurricane - Add Debris
    ###################################
                # add debris
                try:
                    # populate and format values
                    bwTons = self.addCommas(
                        results['DebrisBW'].sum(), abbreviate=True)
                    csTons = self.addCommas(
                        results['DebrisCS'].sum(), abbreviate=True)
                    treeTons = self.addCommas(
                        results['DebrisTree'].sum(), abbreviate=True)
                    eligibleTreeTons = self.addCommas(
                        results['DebrisEligibleTree'].sum(), abbreviate=True)
                    bwTruckLoads = self.addCommas(
                        results['DebrisBW'].sum() * tonsToTruckLoadsCoef, abbreviate=True)
                    csTruckLoads = self.addCommas(
                        results['DebrisCS'].sum() * tonsToTruckLoadsCoef, abbreviate=True)
                    treeTruckLoads = self.addCommas(
                        results['DebrisTree'].sum() * tonsToTruckLoadsCoef, abbreviate=True)
                    eligibleTreeTruckLoads = self.addCommas(
                        results['DebrisEligibleTree'].sum() * tonsToTruckLoadsCoef, abbreviate=True)
                    # populate totals
                    totalTons = self.addCommas(
                        results['DebrisTotal'].sum(), abbreviate=True)
                    totalTruckLoads = self.addCommas(
                        results['DebrisTotal'].sum() * tonsToTruckLoadsCoef, abbreviate=True)
                    total = totalTons + ' Tons/' + totalTruckLoads + ' Truck Loads'
                    # build data dictionary
                    data = {'Debris Type': ['Brick, Wood, and Others', 'Contrete & Steel', 'Tree', 'Eligible Tree'], 'Tons': [
                        bwTons, csTons, treeTons, eligibleTreeTons], 'Truck Loads': [bwTruckLoads, csTruckLoads, treeTruckLoads, eligibleTreeTruckLoads]}
                    # create DataFrame from data dictionary
                    debris = pd.DataFrame(
                        data, columns=['Debris Type', 'Tons', 'Truck Loads'])
                    hurDataDictionary['debris_type_1'] =  debris['Debris Type'][0]
                    hurDataDictionary['debris_tons_1'] =  totalTons                   #TODO: Check if we want truck loads amount - BC
                    hurDataDictionary['total_debris'] =  total
                    self.addTable(debris, 'Debris', total, 'right')
                    self.mergePdfs()        # Added - BC
                    self.mergeToTemplate()  # Added - BC
                    self.writeFillablePdf(hurDataDictionary, path) # Added - BC
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

###################################################
        # Tsunami
###################################################
            if hazard == 'tsunami':
                tsDataDictionary = {}
                tsDataDictionary['title'] =  self.title
                tsDataDictionary['date'] = 'Hazus Run: {}'.format(datetime.datetime.now().strftime('%m-%d-%Y').lstrip('0'))
                # get bulk of results
                try:
                    results = self._Report__getResults()
                    results = results.addGeometry()
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

                # add building damage by occupancy
                try:
                    buildingDamageByOccupancy = self._Report__getBuildingDamageByOccupancy()
                    # create category column
                    buildingDamageByOccupancy['xCol'] = [
                        x[0:3] for x in buildingDamageByOccupancy['Occupancy']]
                    # create new columns for major and destroyed
                    buildingDamageByOccupancy['Major & Destroyed'] = buildingDamageByOccupancy['Major'] + \
                        buildingDamageByOccupancy['Destroyed']
                    # list columns to group for each category
                    yCols = ['Affected', 'Minor', 'Major & Destroyed']
                    self.addHistogram(buildingDamageByOccupancy, 'xCol', yCols,
                                      'Building Damage By Occupancy', 'Buildings', 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

    ###################################
            # Tsunami Economic Loss
    ###################################
                # add economic loss
                try:
                    counties = self.getCounties()
                    economicResults = results[['block', 'EconLoss']]
                    economicResults['countyfips'] = economicResults.block.str[:5]
                    economicLoss = pd.merge(economicResults, counties, how="inner", on=['countyfips'])
                    economicLoss.drop(['size', 'block', 'countyfips', 'geometry', 'crs'], axis=1, inplace=True)
                    economicLoss.columns = ['EconLoss', 'Top Counties', 'State']
                    # populate total
                    total = self.addCommas(economicLoss['EconLoss'].sum(), truncate=True, abbreviate=True)
                    # limit rows to the highest values
                    economicLoss = economicLoss.sort_values('EconLoss', ascending=False)[0:tableRowLimit]
                    # format values
                    economicLoss['EconLoss'] = [self.toDollars(
                        x, abbreviate=True) for x in economicLoss['EconLoss']]
                  # TODO: Use for fillable PDF & add to dictionary - create a function for this - BC
                    tsDataDictionary['econloss_county_1'] =  economicLoss['Top Counties'].iloc[0]
                    tsDataDictionary['econloss_county_2'] =  economicLoss['Top Counties'].iloc[1]
                    tsDataDictionary['econloss_county_3'] =  economicLoss['Top Counties'].iloc[2]
                    tsDataDictionary['econloss_county_4'] =  economicLoss['Top Counties'].iloc[3]
                    tsDataDictionary['econloss_county_5'] =  economicLoss['Top Counties'].iloc[4]
                    tsDataDictionary['econloss_county_6'] =  economicLoss['Top Counties'].iloc[5]
                    tsDataDictionary['econloss_county_7'] =  economicLoss['Top Counties'].iloc[6]
                    tsDataDictionary['econloss_state_1'] =  economicLoss['State'].iloc[0]
                    tsDataDictionary['econloss_state_2'] =  economicLoss['State'].iloc[1]
                    tsDataDictionary['econloss_state_3'] =  economicLoss['State'].iloc[2]
                    tsDataDictionary['econloss_state_4'] =  economicLoss['State'].iloc[3]
                    tsDataDictionary['econloss_state_5'] =  economicLoss['State'].iloc[4]
                    tsDataDictionary['econloss_state_6'] =  economicLoss['State'].iloc[5]
                    tsDataDictionary['econloss_state_7'] =  economicLoss['State'].iloc[6]
                    tsDataDictionary['econloss_total_1'] =  economicLoss['EconLoss'].iloc[0]
                    tsDataDictionary['econloss_total_2'] =  economicLoss['EconLoss'].iloc[1]
                    tsDataDictionary['econloss_total_3'] =  economicLoss['EconLoss'].iloc[2]
                    tsDataDictionary['econloss_total_4'] =  economicLoss['EconLoss'].iloc[3]
                    tsDataDictionary['econloss_total_5'] =  economicLoss['EconLoss'].iloc[4]
                    tsDataDictionary['econloss_total_6'] =  economicLoss['EconLoss'].iloc[5]
                    tsDataDictionary['econloss_total_7'] =  economicLoss['EconLoss'].iloc[6]
                    tsDataDictionary['total_econloss'] =  total
                  #  'total_econloss': total - Add to table
                    self.addTable(
                        economicLoss, 'Total Economic Loss', total, 'left')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
    ####################################################
            # Tsunami - Injuries and Fatalities
    ####################################################
                # add injuries and fatatilies
                try:
                    counties = self.getCounties()
                    injuries = self._Report__getInjuries()
                    fatalities = self._Report__getFatalities()
                    injuriesAndFatatiliesResults = injuries.merge(fatalities, on='block',how='outer')
                    injuriesAndFatatiliesResults = self._Report__getFatalities()
                    injuriesAndFatatiliesResults['countyfips'] = injuriesAndFatatiliesResults.block.str[:5]
                    injuriesAndFatatilies = pd.merge(injuriesAndFatatiliesResults, counties, how="inner", on=['countyfips'])
                    injuriesAndFatatilies.drop(['size', 'countyfips', 'geometry', 'crs', 'block'], axis=1, inplace=True)
                    #injuriesAndFatatilies.columns = ['Top Counties', 'State']                   # TODO: Move down - BC
                    injuriesAndFatatilies['Injuries Day'] = results['Injuries_DayFair'] + \
                        results['Injuries_DayGood'] + \
                        results['Injuries_DayPoor']
                    injuriesAndFatatilies['Injuries Night'] = results['Injuries_NightFair'] + \
                        results['Injuries_NightGood'] + \
                        results['Injuries_NightPoor']
                    injuriesAndFatatilies['Fatalities Day'] = results['Fatalities_DayPoor'] + \
                        results['Fatalities_DayGood']  + \
                        results['Fatalities_DayFair']
                    injuriesAndFatatilies['Fatalities Night'] = results['Fatalities_NightPoor'] + \
                        results['Fatalities_NightGood'] + \
                        results['Fatalities_NightFair']
                    injuriesAndFatatilies.drop(['Fatalities_DayFair', 'Fatalities_DayGood', 'Fatalities_DayPoor', 'Fatalities_NightFair', 'Fatalities_NightGood', 'Fatalities_NightPoor'], axis=1, inplace=True)
                    injuriesAndFatatilies.columns = ['Top Counties', 'State', 'Injuries Day', 'Injuries Night', 'Fatalities Day', 'Fatalities Night']                   # TODO: Move down - BC
                    # populate totals
                    totalDay = self.addCommas(
                        (injuriesAndFatatilies['Injuries Day'] + injuriesAndFatatilies['Fatalities Day']).sum(), abbreviate=True) + ' Day'
                    totalNight = self.addCommas(
                        (injuriesAndFatatilies['Injuries Night'] + injuriesAndFatatilies['Fatalities Night']).sum(), abbreviate=True) + ' Night'
                    total = totalDay + '/' + totalNight
                    # limit rows to the highest values
                    injuriesAndFatatilies = injuriesAndFatatilies.sort_values(
                        'Injuries Day', ascending=False)[0:tableRowLimit]
                    # format values
                    for column in injuriesAndFatatilies:
                        if column not in ['Top Counties', 'State']:
                            injuriesAndFatatilies[column] = [self.addCommas(
                                x, abbreviate=True) for x in injuriesAndFatatilies[column]]
                    tsDataDictionary['nonfatal_county_1'] =  injuriesAndFatatilies['Top Counties'].iloc[0]
                    tsDataDictionary['nonfatal_county_2'] =  injuriesAndFatatilies['Top Counties'].iloc[1]
                    tsDataDictionary['nonfatal_county_3'] =  injuriesAndFatatilies['Top Counties'].iloc[2]
                    tsDataDictionary['nonfatal_county_4'] =  injuriesAndFatatilies['Top Counties'].iloc[3]
                    tsDataDictionary['nonfatal_county_5'] =  injuriesAndFatatilies['Top Counties'].iloc[4]
                    tsDataDictionary['nonfatal_county_6'] =  injuriesAndFatatilies['Top Counties'].iloc[5]
                    tsDataDictionary['nonfatal_county_7'] =  injuriesAndFatatilies['Top Counties'].iloc[6]
                    tsDataDictionary['nonfatal_state_1'] =  injuriesAndFatatilies['State'].iloc[0]
                    tsDataDictionary['nonfatal_state_2'] =  injuriesAndFatatilies['State'].iloc[1]
                    tsDataDictionary['nonfatal_state_3'] =  injuriesAndFatatilies['State'].iloc[2]
                    tsDataDictionary['nonfatal_state_4'] =  injuriesAndFatatilies['State'].iloc[3]
                    tsDataDictionary['nonfatal_state_5'] =  injuriesAndFatatilies['State'].iloc[4]
                    tsDataDictionary['nonfatal_state_6'] =  injuriesAndFatatilies['State'].iloc[5]
                    tsDataDictionary['nonfatal_state_7'] =  injuriesAndFatatilies['State'].iloc[6]
                    tsDataDictionary['nonfatal_pop_1'] =  injuriesAndFatatilies['Injuries Day'].iloc[0] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[0]
                    tsDataDictionary['nonfatal_pop_2'] =  injuriesAndFatatilies['Injuries Day'].iloc[1] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[1]
                    tsDataDictionary['nonfatal_pop_3'] =  injuriesAndFatatilies['Injuries Day'].iloc[2] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[2]
                    tsDataDictionary['nonfatal_pop_4'] =  injuriesAndFatatilies['Injuries Day'].iloc[3] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[3]
                    tsDataDictionary['nonfatal_pop_5'] =  injuriesAndFatatilies['Injuries Day'].iloc[4] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[4]
                    tsDataDictionary['nonfatal_pop_6'] =  injuriesAndFatatilies['Injuries Day'].iloc[5] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[5]
                    tsDataDictionary['nonfatal_pop_7'] =  injuriesAndFatatilies['Injuries Day'].iloc[6] + '/' + injuriesAndFatatilies['Injuries Night'].iloc[6]
                    tsDataDictionary['nonfatal_injuries_1'] =  injuriesAndFatatilies['Fatalities Day'].iloc[0]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[0]
                    tsDataDictionary['nonfatal_injuries_2'] =  injuriesAndFatatilies['Fatalities Day'].iloc[1]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[1]
                    tsDataDictionary['nonfatal_injuries_3'] =  injuriesAndFatatilies['Fatalities Day'].iloc[2]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[2]
                    tsDataDictionary['nonfatal_injuries_4'] =  injuriesAndFatatilies['Fatalities Day'].iloc[3]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[3]
                    tsDataDictionary['nonfatal_injuries_5'] =  injuriesAndFatatilies['Fatalities Day'].iloc[4]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[4]
                    tsDataDictionary['nonfatal_injuries_6'] =  injuriesAndFatatilies['Fatalities Day'].iloc[5]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[5]
                    tsDataDictionary['nonfatal_injuries_7'] =  injuriesAndFatatilies['Fatalities Day'].iloc[6]  + '/' + injuriesAndFatatilies['Fatalities Night'].iloc[6]
                    tsDataDictionary['total_injuries'] =  total
                    self.addTable(injuriesAndFatatilies, 'Injuries and Fatatilies', total, 'left')
                    
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

    ###################################
            # Tsunami - Economic Loss Map
    ###################################
                # add economic loss map
                try:
                    economicLoss = results[['block', 'EconLoss', 'geometry']]
                    # convert to GeoDataFrame
                    economicLoss.geometry = economicLoss.geometry.apply(loads)
                    gdf = gpd.GeoDataFrame(economicLoss)
                    self.addMap(gdf, title='Economic Loss by Census Block (USD)',
                                column='right', field='EconLoss', cmap='OrRd')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass

    #################################################
            # Tsunami - Travel Time to Safety Map
    #################################################
                # add travel time to safety map
                try:
                    travelTimeToSafety = self._Report__getTravelTimeToSafety()
                    title = 'Travel Time to Safety (minutes)'
                    self.addMap(travelTimeToSafety, title=title,
                                column='right', field='travelTimeOver65yo', formatTicks=False, cmap='YlOrRd')
                except Exception as e:
                    print('\n')
                    print(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(fname)
                    print(exc_type, exc_tb.tb_lineno)
                    print('\n')
                    pass
                self.mergePdfs()        # Added - BC
                self.mergeToTemplate()  # Added - BC
                self.writeFillablePdf(tsDataDictionary, path) # Added - BC
        except Exception as e:
            print('\n')
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(fname)
            print(exc_type, exc_tb.tb_lineno)
            print('\n')
            raise

###########################
# Fillable PDF Functions
###########################

    def convertPngToPdf(self, src, title, x, y, scale):
        try:
            watermarkFile =  os.path.join(os.getcwd(), self._tempDirectory, (title.replace(' ', '-') + '.pdf'))
            c = canvas.Canvas(watermarkFile)
            c.scale(1/scale, 1/scale)
            c.drawImage(src, x, y)
            c.save()
        except Exception as e:
            print('\n')
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(fname)
            print(exc_type, exc_tb.tb_lineno)
            print('\n')

    def mergePdfs(self):
        ''' Merges all the pdf files in Hazpy temp directory '''
        try:
            merger = PdfFileMerger()
            tempDirectory = os.path.join(os.getcwd(), self._tempDirectory)
            pdfFiles = [os.path.join(tempDirectory, f) for f in os.listdir(tempDirectory) if f.endswith('.pdf')]
            [merger.append(pdf) for pdf in pdfFiles]
            images = os.path.join(tempDirectory, 'Images.pdf')
            with open(images, "wb") as new_file:
                merger.write(new_file)
            imagesFile = open(os.path.join(tempDirectory, 'Images.pdf'), 'rb')
            output = PdfFileWriter()
            input = PdfFileReader(imagesFile)
            page1 = input.getPage(0)
            [page1.mergePage(input.getPage(page_num)) for page_num in range(input.numPages) if page_num > 0]
            output.addPage(page1)
            with open(os.path.join(tempDirectory, 'Merged.pdf'), 'wb') as outputstream:
                output.write(outputstream)
            imagesFile.close()
        except Exception as e:
            print('\n')
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(fname)
            print(exc_type, exc_tb.tb_lineno)
            print('\n')

    def mergeToTemplate(self):
        templateFile = open(os.path.join(os.getcwd(), self.templateFillableLocation, self.hazard.title() + '.pdf'), 'rb')
        mergedFile = os.path.join(os.getcwd(), self._tempDirectory, 'Merged.pdf')
        imagesFile = open(mergedFile, 'rb')
        output = PdfFileWriter()
        catalog = output._root_object
        if 'AcroForm' not in catalog:
            output._root_object.update({NameObject("/AcroForm"): IndirectObject(len(output._objects), 0, output)})
        need_appearances = NameObject("/NeedAppearances")
        output._root_object["/AcroForm"][need_appearances] = BooleanObject(True)
        imageInput = PdfFileReader(imagesFile).getPage(0)
        templateInput = PdfFileReader(templateFile).getPage(0)
        templateInput.mergePage(imageInput)
        output.addPage(templateInput)
        fillableReportWithImages = os.path.join(os.getcwd(), self._tempDirectory, 'fillableReportWithImages.pdf')
        with open(fillableReportWithImages, 'wb') as outputstream:
            output.write(outputstream)
        templateFile.close()
        imagesFile.close()

    def setNeededAppearances(self, writer):
        catalog = writer._root_object
        # get the AcroForm tree and add "/NeedAppearances attribute
        if "/AcroForm" not in catalog:
            writer._root_object.update({NameObject("/AcroForm"): IndirectObject(len(writer._objects), 0, writer)})
        need_appearances = NameObject("/NeedAppearances")
        writer._root_object["/AcroForm"][need_appearances] = BooleanObject(True)
        return writer

    def writeFillablePdf(self, data_dict, path):
        outputPdf = path.replace('report_summary', self.title.replace(' ', '-'))
        reportTemplate = os.path.join(os.getcwd(), self._tempDirectory, 'fillableReportWithImages.pdf')
        with open(reportTemplate, 'rb') as inputStream:
            pdfReader = PdfFileReader(inputStream, strict=False)
            if "/AcroForm" in pdfReader.trailer["/Root"]:
                pdfReader.trailer["/Root"]["/AcroForm"].update({NameObject("/NeedAppearances"): BooleanObject(True)})
            pdfFileWriter = PdfFileWriter()
            self.setNeededAppearances(pdfFileWriter)
            if "/AcroForm" in pdfFileWriter._root_object:
                pdfFileWriter._root_object["/AcroForm"].update({NameObject("/NeedAppearances"): BooleanObject(True)})
                pdfFileWriter.addPage(pdfReader.getPage(0))
                pdfFileWriter.updatePageFormFieldValues(pdfFileWriter.getPage(0), data_dict)
            with open(outputPdf, 'wb') as outputStream:
                pdfFileWriter.write(outputStream)


# TODO: Adjust image scaling    - BC
# TODO: Fix position of earthquake chart? or just don't include - BC
# TODO: Edit try/excepts - BC
# TODO: Verify data matches database - BC
# TODO: Rollup values when county name is not distinct - BC
# TODO: Adjust map colors to match gradient legend - BC
# TODO: Adjust color/look of maps --> to match hazus PDF maps - BC
# TODO: Fill in Tsuanmi debris boxes - BC
# TODO: Push code to repos (export & hazpy) - BC
# TODO: Run code coverage to see what modules are being used - BC