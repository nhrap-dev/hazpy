from xhtml2pdf import pisa
import os
import pandas as pd

import geopandas as gpd
from shapely.wkt import loads

from matplotlib import pyplot as plt
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
from mpl_toolkits.axes_grid1.axes_divider import make_axes_locatable
import matplotlib.patheffects as pe
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon
from matplotlib.colors import LinearSegmentedColormap
import shapely
from jenkspy import jenks_breaks as nb
import numpy as np
import shutil

import contextily as ctx
import sys


"""
------testing-----------

import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from hazpy import legacy
sr = legacy.StudyRegion('hu_test')
cs = sr.getCounties()
hd = sr.getHazardDictionary()
hdf = hd[list(hd.keys())[0]]

fig = plt.figure(figsize=(3, 3), dpi=300)
ax = fig.gca()
hdf.plot(column='PARAMVALUE', cmap='Blues', ax=ax)
cs.plot(facecolor="none", edgecolor="darkgrey", linewidth=0.2, ax=ax)

annotationDf = cs.sort_values('size', ascending=False)[0:5]
annotationDf.geometry.iloc[0].centroid
annotationDf['centroid'] = [x.centroid for x in annotationDf['geometry']]
for row in range(len(annotationDf)):
    name = annotationDf.iloc[row]['name']
    coords = annotationDf.iloc[row]['centroid']
    plt.annotate(s=name, xy=[coords.x, coords.y], horizontalalignment='center', size = 3,
                 color='white', path_effects=[pe.withStroke(linewidth=1, foreground='#404040')])

ax.axis('scaled')
ax.axis('off')
fig.show()

el = sr.getEconomicLoss()
el['total'] = '555'
df = el.iloc[0:7]
el = el.addGeometry()
el['geometry'] = el['geometry'].apply(wkt.loads)
gdf = gpd.GeoDataFrame(el, geometry='geometry')


hazard_colors = {
    '0': {'lowValue': 0.0, 'highValue': 0, 'color': '#ffffff'},
    '1': {'lowValue': 0.0, 'highValue': 0.0017, 'color': '#dfe6fe'},
    '2': {'lowValue': 0.0017, 'highValue': 0.0078, 'color': '#dfe6fe'},
    '3': {'lowValue': 0.0078, 'highValue': 0.014, 'color': '#82f9fb'},
    '4': {'lowValue': 0.014, 'highValue': 0.039, 'color': '#7efbdf'},
    '5': {'lowValue': 0.039, 'highValue': 0.092, 'color': '#95f879'},
    '6': {'lowValue': 0.092, 'highValue': 0.18, 'color': '#f7f835'},
    '7': {'lowValue': 0.18, 'highValue': 0.34, 'color': '#fdca2c'},
    '8': {'lowValue': 0.34, 'highValue': 0.65, 'color': '#ff701f'},
    '9': {'lowValue': 0.65, 'highValue': 1.24, 'color': '#ec2516'},
    '10': {'lowValue': 1.24, 'highValue': 2, 'color': '#c81e11'}
}

breaks = [hazard_colors[x]['highValue'] for x in hazard_colors][1:]
color_vals = [gdf.iloc[[x]]['PGA'][0] for x in idx]

fig = plt.figure(figsize=(2.74, 2.46), dpi=600)
ax = fig.gca()
breaks = 5
color_vals = nb(gdf.EconLoss, breaks)
color_array = pd.cut(color_vals, bins=(list(breaks)), labels=[
                     x[0] + 1 for x in enumerate(list(breaks))][0:-1])
color_array = pd.Series(pd.to_numeric(color_array)).fillna(0)
poly.set(array=color_array, cmap='Reds')
ax.add_collection(poly)
boundaries.set(facecolor='None', edgecolor='#303030', linewidth=0.3, alpha=0.5)
ax.add_collection(boundaries)
ax.margins(x=0, y=0.1)
ax.axis('off')
ax.axis('scaled')
fig.tight_layout(pad=0, h_pad=None, w_pad=None, rect=None)
fig.show()




from hazpy import legacy
sr = legacy.StudyRegion('eq_test_AK')
el = sr.getEconomicLoss()
el['total'] = '555'
df = el.iloc[0:7]
report = legacy.Report('Earthquake Wesley',
                       'Mean son of a gun', icon='tornado')

report.addTable(df, 'Economic Loss', '$400 B', 'left')
report.addTable(df, 'Shelter Needs', '5 k people', 'left')
report.addImage("C:/projects/HazusTools/HEU/test.png",
                'Time to Shelter', 'left')
# report.addImage("C:/projects/HazusTools/HEU/test.png",'Time to Shelter', 'right')
# report.addTable(df, 'Economic Loss', '$400 B', 'right')
report.addTable(df, 'Shelter Needs', '5 k people', 'right')

report.save('C:/Users/jrainesi/Downloads/test_report.pdf')

"""


class Report():
    """ -- A StudyRegion helper class --
    Creates a report object \n

    Keyword Arguments: \n
        title: str -- report title \n
        subtitle: str -- report subtitle \n
        icon: str -- report hazard icon (choices: 'earthquake', 'flood', 'hurricane', 'tsunami')

    """

    def __init__(self, studyRegionClass, title, subtitle, icon):
        self.__getResults = studyRegionClass.getResults
        self.__getEssentialFacilities = studyRegionClass.getEssentialFacilities
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
        # TODO hazard icons
        self.icon = self.assets[icon]
        self.template = ''
        self.disclaimer = """The estimates of social and economic impacts contained in this report were produced using Hazus loss estimation methodology software which is based on current scientific and engineering knowledge. There are uncertainties inherent in any loss estimation
            technique. Therefore, there may be significant differences between the modeled results contained in this report and the actual social and economic losses following a specific earthquake. These results can be improved by using enhanced inventory, geotechnical,
            and observed ground motion data."""
        self.getCounties = studyRegionClass.getCounties
        self._tempDirectory = 'hazpy-report-temp'

    def abbreviate(self, number):
        # TODO debug
        try:
            breakpoint()
            digits = 0
            f_str = str("{:,}".format(round(number, digits)))
            if ('.' in f_str) and (digits == 0):
                f_str = f_str.split('.')[0]
            if (number > 1000) and (number < 1000000):
                split = f_str.split(',')
                f_str = split[0] + '.' + split[1][0:-1] + ' K'
            if (number > 1000000) and (number < 1000000000):
                split = f_str.split(',')
                f_str = split[0] + '.' + split[1][0:-1] + ' M'
            if (number > 1000000000) and (number < 1000000000000):
                split = f_str.split(',')
                f_str = split[0] + '.' + split[1][0:-1] + ' B'
            return f_str
        except:
            return str(number)

    def addCommas(self, number, abbreviate=False, truncate=False):
        if truncate:
            number = int(round(number))
        if abbreviate:
            number = self.abbreviate(str(number))
        return "{:,}".format(number)

    def toDollars(self, number, abbreviate=False, truncate=False):
        # TODO debug
        breakpoint()
        if truncate:
            number = int(round(number))
        if abbreviate:
            number = self.abbreviate(number)
            dollars = '$'+"{:,}".format(str(number))
        else:
            dollars = '$'+"{:,}".format(str(number))
            dollarsSplit = dollars.split('.')
            dollars = ''.join([dollarsSplit[0], dollarsSplit[1][0:1]])
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
                            padding-top: 0;
                            padding-bottom: 0;
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
                            width: 512pt;
                            padding-top: 0;
                            padding-bottom: 0;
                            padding-left: 5px;
                            padding-right: 5px;
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
                            width: 60%;
                        }
                        .results_table_header_title_solo {
                            color: #fff;
                            text-align: left;
                            padding-top: 3px;
                            padding-bottom: 1px;
                            width: 512pt;
                        }
                        .results_table_header_total {
                            color: #fff;
                            text-align: center;
                            padding-top: 3px;
                            padding-bottom: 1px;
                        }
                        .results_table_header_number {
                            color: #fff;
                            text-align: left;
                            padding-top: 3px;
                            padding-bottom: 1px;
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
        """ Adds a table to the report \n

        Keyword Arguments: \n
            df: pandas dataframe -- expects a StudyRegionDataFrame \n
            title: str -- report title \n
            subtitle: str -- report subtitle \n
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

    def addImage(self, src, title, column):
        """ Adds image block to the report \n

        Keyword Arguments: \n
            src: str -- the path and filename of the image \n
            title: str -- the title of the image \n
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
        if column == 'left':
            self.columnLeft = self.columnLeft + template
        if column == 'right':
            self.columnRight = self.columnRight + template

    def addMap(self, gdf, field, title, column, annotate=True, legend=True, cmap='Blues'):
        """ Adds a map to the report \n

        Keyword Arguments: \n
            gdf: geopandas geodataframe -- a geodataframe containing the data to be mapped \n
            field: str -- the field for the choropleth \n
            title: str -- section title in the report \n
            column: str -- which column in the report to add to (options: 'left', 'right') \n
            annotate (optional): bool -- adds top 5 most populated city labels to map \n
            legend (optional): bool -- adds a colorbar to the map \n
            cmap (optional): str -- the colormap used for the choropleth; default = 'Blues'
        """
        """
        -- testing --
        from hazpy import legacy
        sr = legacy.StudyRegion('eq_test_AK')
        sr = legacy.StudyRegion('hu_test')
        el = sr.getEconomicLoss()
        gdf = el.addGeometry()
        sr.report.addMap(gdf, title='Economic Loss',
                         column='left', field='EconLoss')
        sr.report.save('C:/Users/jrainesi/Downloads/testReport.pdf')
        """
        try:
            fig = plt.figure(figsize=(3, 3), dpi=300)
            ax = fig.gca()

            try:
                breakpoint()
                gdf.plot(column=field, cmap='Blues', ax=ax)
            except:
                gdf['geometry'] = gdf['geometry'].apply(loads)
                gdf = gpd.GeoDataFrame(gdf, geometry='geometry')
                gdf.plot(column=field, cmap='Blues', ax=ax)
            if legend == True:
                sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(
                    vmin=gdf[field].min(), vmax=gdf[field].max()))
                sm._A = []

                # colorbarParams = inset_axes(ax, width="99%", height="5%", loc='upper center', bbox_to_anchor=(0.5, 0.05, 0.5, 0.05))
                divider = make_axes_locatable(ax)
                cax = divider.append_axes("top", size="10%", pad="20%")
                cb = fig.colorbar(sm, cax=cax, orientation="horizontal")
                cb.outline.set_visible(False)
                fontsize = 3
                fig.axes[0].tick_params(labelsize=fontsize, size=fontsize)
                fig.axes[1].tick_params(labelsize=fontsize, size=fontsize)

            if annotate == True:
                counties = self.getCounties()
                counties.plot(facecolor="none",
                              edgecolor="darkgrey", linewidth=0.2, ax=ax)
                annotationDf = counties.sort_values(
                    'size', ascending=False)[0:5]
                annotationDf = annotationDf.sort_values(
                    'size', ascending=True)

                annotationDf['centroid'] = [
                    x.centroid for x in annotationDf['geometry']]

                maxSize = annotationDf['size'].max()
                topFontSize = 2.5
                annotationDf['fontSize'] = topFontSize * \
                    (annotationDf['size'] / annotationDf['size'].max()) + (
                        topFontSize - ((annotationDf['size'] / annotationDf['size'].max()) * 2))
                for row in range(len(annotationDf)):
                    name = annotationDf.iloc[row]['name']
                    coords = annotationDf.iloc[row]['centroid']
                    ax.annotate(s=name, xy=(float(coords.x), float(coords.y)), horizontalalignment='center', size=annotationDf.iloc[row]['fontSize'], color='white', path_effects=[
                        pe.withStroke(linewidth=1, foreground='#404040')])

            ax.axis('scaled')
            ax.axis('off')
            if not os.path.isdir(os.getcwd() + '/' + self._tempDirectory):
                os.mkdir(os.getcwd() + '/' + self._tempDirectory)
            src = os.getcwd() + '/' + self._tempDirectory + '/'+field+".png"
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
        except:
            fig.clf()
            plt.clf()
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def save(self, path):
        # open output file for writing (truncated binary)
        self.updateTemplate()
        result_file = open(path, "w+b")

        # convert HTML to PDF
        pisa_status = pisa.CreatePDF(
            self.template,
            dest=result_file)

        # close output file
        result_file.close()

        os.startfile(path)
        shutil.rmtree(os.getcwd() + '/' + self._tempDirectory)

        # return False on success and True on errors
        return pisa_status.err

    def saveDefault(self, hazard):
        """ Adds image block to the report \n

        Keyword Arguments: \n
            src: str -- the path and filename of the image \n
            title: str -- the title of the image \n
            column: str -- which column in the report to add to (options: 'left', 'right')
        """
        """
        --- testing ---
        from hazpy import legacy
        sr = legacy.StudyRegion('eq_test_AK')
        sr.report.saveDefault('earthquake')
        # sr = legacy.StudyRegion('hu_test')
        # el = sr.getEconomicLoss()
        # gdf = el.addGeometry()

        """
        try:
            if hazard == 'earthquake':
                breakpoint()
                results = self._Report__getResults()
                results = results.addCensusTracts()
                essentialFacilities = self._Report__getEssentialFacilities()

                economicLoss = results[['tract', 'EconLoss']]
                totalEconomicLoss = self.addCommas(
                    economicLoss['EconLoss'].sum())
                self.toDollars(economicLoss['EconLoss'].sum(), truncate=False)
                # el = sr.getEconomicLoss()
                # el['total'] = '555'
                # df = el.iloc[0:7]

                # report.addTable(df, 'Economic Loss', '$400 B', 'left')
                # report.addTable(df, 'Shelter Needs', '5 k people', 'left')
                # report.addImage(
                # "C:/projects/HazusTools/HEU/test.png", 'Time to Shelter', 'left')
                # report.addImage("C:/projects/HazusTools/HEU/test.png",'Time to Shelter', 'right')
                # report.addTable(df, 'Economic Loss', '$400 B', 'right')
                # report.addTable(df, 'Shelter Needs', '5 k people', 'right')

                # report.save('C:/Users/jrainesi/Downloads/test_report.pdf')
            if hazard == 'flood':
                pass
            if hazard == 'hurricane':
                pass
            if hazard == 'tsunami':
                pass
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
