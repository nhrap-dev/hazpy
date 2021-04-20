from hazpy.legacy import HazusPackageRegion
from pathlib import Path
import os

#Create HazusPackageRegion object...
#file = r'C:\workspace\hprfiles\NorCal-BayArea_SanAndreasM7-8.hpr'
file = r'C:\workspace\hprfiles\banMO.hpr'

hpr = HazusPackageRegion(hprFilePath=file, outputDir=r'C:\workspace')

print(hpr.hprFilePath)
print(hpr.outputDir)
print(hpr.tempDir)
print(hpr.hprComment)
print(hpr.HazusVersion)
print(hpr.Hazards)
print()

try:
    hpr.restoreHPR()
except Exception as e:
    print(e)

hpr.getHazardsScenariosReturnPeriods()

#create a directory for the output folders...
outputPath = hpr.outputDir
if not os.path.exists(outputPath):
    os.mkdir(outputPath)
            

#iterate over the hazard, scenario, returnperiod available combinations...
for hazard in hpr.HazardsScenariosReturnPeriods:
    print(hazard['Hazard'])
    for scenario in hazard['Scenarios']:
        print(scenario['ScenarioName'])
        for returnPeriod in scenario['ReturnPeriods']:
            print(returnPeriod)

            #set hpr hazard, scenario, returnPeriod...
            hpr.hazard = hazard['Hazard']
            hpr.scenario = scenario['ScenarioName']
            hpr.returnPeriod = returnPeriod
            print(hpr.hazard, hpr.scenario, hpr.returnPeriod)

            #get bulk of results...
            try:
                print('Get bulk of results.')
                results = hpr.getResults()
                essentialFacilities = hpr.getEssentialFacilities()
                if len(results) < 1:
                    print('No results found. Please check your Hazus Package Region and try again.')
            except Exception as e:
                print(e)

            #create a directory for the output folders like "HPR>Hazard>Scenario>STAGE_ReturnPeriod"...
            #returnperiod divider is set to STAGE for FIMs/PTS but can be changed back to RP for other.
            exportPath = Path.joinpath(Path(outputPath), str(hazard['Hazard']).strip(), str(scenario['ScenarioName']).strip(), 'STAGE_' + str(returnPeriod).strip())
            Path(exportPath).mkdir(parents=True, exist_ok=True) #this may make the earlier HPR dir creation redundant

            #export Hazus Package Region to csv...
            try:
                try:
                    results.toCSV(Path.joinpath(exportPath, 'results.csv'))
                except Exception as e:
                    print('Base results not available to export to csv.')
                    print(e)
                    
                try:
                    print('Writing building damage by occupancy to CSV')
                    buildingDamageByOccupancy = hpr.getBuildingDamageByOccupancy()
                    buildingDamageByOccupancy.toCSV(Path.joinpath(exportPath, 'building_damage_by_occupancy.csv'))
                except Exception as e:
                    print('Building damage by occupancy not available to export to csv.')
                    print(e)
                    
                try:
                    print('Writing building damage by type to CSV')
                    buildingDamageByType = hpr.getBuildingDamageByType()
                    buildingDamageByType.toCSV(Path.joinpath(exportPath,'building_damage_by_type.csv'))
                except Exception as e:
                    print('Building damage by type not available to export to csv.')
                    print(e)
                    
                try:
                    print('Writing damaged facilities to CSV')
                    essentialFacilities.toCSV(Path.joinpath(exportPath, 'damaged_facilities.csv'))
                except Exception as e:
                    print('Damaged facilities not available to export to csv.')
                    print(e)
            except Exception as e:
                print('Unexpected error exporting CSVs')
                print(e)
                    
            #export Hazus Package Region to Shapefile
            try:
                try:
                    results.toShapefile(Path.joinpath(exportPath, 'results.shp'))
                except Exception as e:
                    print('Base results not available to export to shapefile.')
                    print(e)
                    
                try:
                    essentialFacilities.toShapefile(Path.joinpath(exportPath, 'damaged_facilities.shp'))
                except Exception as e:
                    print('Damaged facilities not available to export to shapefile.')
                    print(e)
                    
                try:
                    if not 'hazard' in dir():
                        hazard = hpr.getHazardGeoDataFrame()
                    hazard.toShapefile(Path.joinpath(exportPath, 'hazard.shp'))
                except Exception as e:
                    print('Hazard not available to export to shapefile.')
                    print(e)
            except Exception as e:
                print(u"Unexpected error exporting Shapefile: ")
                print(e)
                
            #export Hazus Package Region to GeoJSON
            try:
                try:
                    results.toGeoJSON(Path.joinpath(exportPath, 'results.geojson'))
                except Exception as e:
                    print('Base results not available to export to geojson.')
                    print(e)
                    
                try:
                    essentialFacilities.toGeoJSON(Path.joinpath(exportPath, 'damaged_facilities.geojson'))
                except Exception as e:
                    print('Damaged facilities not available to export to geojson.')
                    print(e)
                    
                try:
                    if not 'hazard' in dir():
                        hazard = hpr.getHazardGeoDataFrame()
                    hazard.toGeoJSON(Path.joinpath(exportPath, 'hazard.geojson'))
                except Exception as e:
                    print('Hazard not available to export to geojson.')
                    print(e)

                try:
                    econloss = hpr.getEconomicLoss()
                    if len(econloss.loc[econloss['EconLoss'] > 0]) > 0:
                        econloss.toHLLGeoJSON(Path.joinpath(exportPath, 'econloss_simpconvexHLL.geojson'))
                    else:
                        print('no econ loss for HLL geojson')
                except Exception as e:
                    print('Convex Hull Simplified Economic loss not available to export to geojson.')
                    print(e)
                        
            except Exception as e:
                print('Hazard not available to export to geojson.')
                
            #export Hazus Package Region to pdf
            try:
                hpr.report = Report(self, self.name, '', self.hazard) #inits with self.hazard
                reportTitle = self.text_reportTitle.get("1.0", 'end-1c')
                if len(reportTitle) > 0:
                    hpr.report.title = reportTitle
                reportSubtitle = self.text_reportSubtitle.get("1.0", 'end-1c')
                if len(reportSubtitle) > 0:
                    hpr.report.subtitle = reportSubtitle
                hpr.report.save(Path.joinpath(exportPath, 'report_summary.pdf'), build=True)
            except Exception as e:
                print(u"Unexpected error exporting the PDF: ")
                print(e)
        print()
        print()

print()

try:
    hpr.dropDB()
except Exception as e:
    print(e)
    
##try:
##    hpr.deleteTempDir()
##except Exception as e:
##    print(e)
