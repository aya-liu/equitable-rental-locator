import pickle
import pandas as pd
import geopandas
from shapely.geometry import Point
import matplotlib.pyplot as plt

# read scraped CHA data
with open("data/CHA_rental_data.obj", "rb") as f:       
    d = pickle.load(f)
cha = pd.DataFrame.from_dict(data = d, orient = "index")
# clean CHA data
cols = ['Address','Monthly Rent','Property Type','Bath','Bed',
        'Voucher Necessary','Availability','Contact','URL','Lat','Long']
cha = cha[cols]
cha.Long = -1 * cha.Long
# correct error
cha.loc["4545145", "Long"] = -87.66593 
cha.loc["4545145", "Lat"] = 41.772175
# convert to GeoDataFrame
cha['Coordinates'] = list(zip(cha.Long, cha.Lat))
cha['Coordinates'] = cha['Coordinates'].apply(Point)
gcha = geopandas.GeoDataFrame(cha, geometry='Coordinates')

blocks_full = geopandas.read_file("data/block-groups.geojson")
blocks = blocks_full[['geometry', 'GEOID']]
blocks.head()

cha_with_geoid = geopandas.sjoin(gcha, blocks, how="left", op='intersects')
cha_with_geoid.head()
shp_filepath = "data/ZillowNeighborhoods-IL.shp"
zillow_neighborhoods = geopandas.read_file(shp_filepath)
zillow_neighborhoods.head()

cha_with_geoid.drop('index_right', axis=1, inplace=True)
cha_geoid_zillow = geopandas.sjoin(cha_with_geoid, zillow_neighborhoods, how="left", op='intersects')
cha_geoid_zillow
cha_geoid_zillow.dropna(axis=0, subset=["index_right"], inplace=True)
cha_geoid_zillow.drop('index_right', axis=1, inplace=True)

#MAPPING STARTS HERE
count_series = cha_geoid_zillow.groupby('GEOID').size()
new_df = pd.DataFrame()
new_df['GEOID'] = count_series.index
new_df['count_properties'] = count_series.values

city_blocks = geopandas.read_file("Boundaries - Census Blocks - 2010.geojson")
city_blocks['GEOID'] = city_blocks['geoid10'].str.slice(start=0, stop=12)
merged = pd.merge(city_blocks, new_df, how="left")
merged = merged.fillna(0)
variable = 'count_properties'
vmin, vmax = 0, 50
fig, ax = plt.subplots(1, figsize=(10, 6))
merged.plot(column=variable, cmap='Blues', scheme='Quantiles', ax=ax)
ax.axis('off')
sm = plt.cm.ScalarMappable(cmap= 'Blues', norm=plt.Normalize(vmin=vmin, vmax=vmax))
sm._A = []
cbar = fig.colorbar(sm)
ax.set_title('Where are CHA Properties Located?', fontdict={'fontsize': '15', 'fontweight' : '3'})
fig.savefig('testmap.png', dpi=300)


