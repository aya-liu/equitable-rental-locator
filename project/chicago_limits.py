import geopandas

shapefile_name = "Boundaries - Census Blocks - 2010.geojson"

def get_city_geoids(shapefile_name):
	city_blocks = geopandas.read_file(shapefile_name)
	city_blocks['GEOID'] = city_blocks['geoid10'].str.slice(start=0, stop=12)
	return city_blocks['GEOID']
