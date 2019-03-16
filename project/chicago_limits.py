import geopandas

shapefile_name = "Boundaries-CensusBlocks-2010.geojson"

def get_city_geoids(shapefile_name):
	city_blocks = geopandas.read_file(shapefile_name)
	city_blocks['GEOID'] = city_blocks['geoid10'].str.slice(start=0, stop=12)
	city_blocks['GEOID'].to_csv('list_of_chi_geoids.csv')

