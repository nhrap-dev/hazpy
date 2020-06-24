from xhtml2pdf import pisa
import os
import pandas as pd

from matplotlib import pyplot as plt
import matplotlib.patheffects as pe
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon
from matplotlib.colors import LinearSegmentedColormap
import shapely
from jenkspy import jenks_breaks as nb
import numpy as np
"""
------testing-----------

from hazpy import legacy
sr = legacy.StudyRegion('eq_test_AK')
el = sr.getEconomicLoss()
el['total'] = '555'
df = el.iloc[0:7]

report = legacy.Report('Earthquake Wesley', 'Mean son of a gun', icon='tornado')

report.addTable(df, 'Economic Loss', '$400 B', 'left')
report.addTable(df, 'Shelter Needs', '5 k people', 'left')
report.addImage("C:/projects/HazusTools/HEU/test.png",
                'Time to Shelter', 'left')
report.addImage("C:/projects/HazusTools/HEU/test.png",
                'Time to Shelter', 'right')
report.addTable(df, 'Economic Loss', '$400 B', 'right')
report.addTable(df, 'Shelter Needs', '5 k people', 'right')

report.save('C:/Users/jrainesi/Downloads/test_report.pdf')

"""


class Report():
    """ Creates a report object \n

    Keyword Arguments: \n
        title: str -- report title \n
        subtitle: str -- report subtitle \n
        icon: str -- report hazard icon (choices: 'earthquake', 'flood', 'hurricane', 'tsunami')

    """

    def __init__(self, title, subtitle, icon):
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
                            height: 5px;
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
                        .header_text {
                            padding-top: 0;
                            padding-bottom: 0;
                            padding-left: 0;
                            padding-right: 0;
                        }
                        .column_left {
                            margin-top: 0;
                            padding-top: 5px;
                            padding-bottom: 0;
                            padding-left: 0;
                            padding-right: 5px;
                        }
                        .column_right {
                            margin-top: 0;
                            padding-top: 5px;
                            padding-bottom: 0;
                            padding-left: 5px;
                            padding-right: 0;
                        }
                        .report_columns {
                            padding-top: 5px;
                            padding-bottom: 5px;
                        }
                        .result_container {
                            padding-top: 5px;
                            padding-bottom: 0;
                            padding-left: 0;
                            padding-right: 0;
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
                                <h1 class="header_text">"""+self.title+"""</h1>
                                <p class="header_text">"""+self.subtitle+"""</p>
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
            """
        if column == 'left':
            self.columnLeft = self.columnLeft + template
        if column == 'right':
            self.columnRight = self.columnRight + template

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

        # return False on success and True on errors
        return pisa_status.err
