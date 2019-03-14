import pickle
import pandas as pd
import geopandas
from shapely.geometry import Point
import matplotlib.pyplot as plt

CHA_RAW_FILE = "data/CHA_rental_data.obj"
BLOCKS_FILE = "data/block-groups.geojson"
ZILLOW_SHP_FILE = "data/ZillowNeighborhoods-IL.shp"


def load_and_clean_cha(CHA_filename):
    '''
    Load and clean CHA data.
    Input:
        (str) file name of the raw CHA housing unit data
    Returns:
        (DataFrame) clean CHA data
    '''
    # load raw CHA data
    with open(CHA_filename, "rb") as f:       
        d = pickle.load(f)
    cha = pd.DataFrame.from_dict(data = d, orient = "index")
    
    # clean CHA data
    cols = ['Address','Monthly Rent','Property Type','Bath','Bed',
        'Voucher Necessary','Availability','Contact','URL','Lat','Long']
    cha = cha[cols]
    cha.Long = -1 * cha.Long

    # correct an one-off location error
    cha.loc["4545145", "Long"] = -87.66593 
    cha.loc["4545145", "Lat"] = 41.772175
    return cha


def convert_to_gdf(df):
    '''
    Convert a DataFrame with "Lat" and "Long" columns to a GeoDataFrame

    Input: a DataFrame object
    Returns: a GeoDataFrame object with coordinates
    '''
    df['Coordinates'] = list(zip(df.Long, df.Lat))
    df['Coordinates'] = df['Coordinates'].apply(Point)
    gdf = geopandas.GeoDataFrame(df, geometry='Coordinates')
    return gdf


def add_blocks_to_cha(gcha, blocks_filename):
    '''
    Add Census block group GEOID to CHA housing unit geodataframe

    Input:
        gcha: (GeoDataFrame) CHA geodataframe with coordinates
        blocks_filename: (str) filename of the block group geojson
    Returns: (GeoDataFrame) CHA geodataframe with block group GEOIDs
    '''
    blocks_full = geopandas.read_file(blocks_filename)
    blocks = blocks_full[['geometry', 'GEOID']]
    cha_with_geoid = geopandas.sjoin(gcha, blocks, how="left", 
                                                    op='intersects')
    cha_with_geoid.drop('index_right', axis=1, inplace=True)
    return cha_with_geoid


def add_zillow_regionid_to_cha(gcha, zillow_filename):
    '''
    Add Zillow RegionID to CHA housing unit geodataframe

    Input:
        gcha: (GeoDataFrame) CHA geodataframe with coordinates
        zillow_filename: (str) filename of the zillow region shapefile  
    Returns: (GeoDataFrame) CHA geodataframe with block group GEOIDs
    '''
    zillow_neighborhoods = geopandas.read_file(zillow_filename)
    cha_geoid_zillow = geopandas.sjoin(gcha, zillow_neighborhoods, 
                                        how="left", op='intersects')
    cha_geoid_zillow = fill_zillow_na(cha_geoid_zillow)
    cha_geoid_zillow.drop('index_right', axis=1, inplace=True)
    return cha_geoid_zillow


def fill_zillow_na(cha_geoid_zillow):
    '''
    '''
    # drop rows with false addresses
    cha_geoid_zillow.drop(index = ['4763759','4796475'], inplace=True)
    # fill invariant NA values
    cha_geoid_zillow.fillna({"State": "IL", "County": "Cook", 
        "City": "Chicago"}, inplace=True)
    # manually look up Zillow Regions on zillow.com  
    unmatches = cha_geoid_zillow[cha_geoid_zillow["index_right"].isna()]
    values = {'4567448': ['Gresham', '269571'],
         '4590237': ['Gresham', '269571'],
         '4618006': ['Cabrini Green', '403302'],
         '4632177': ['Park Manor', '403356'],
         '4640435': ['Marquette Park', '403148'],
         '4646604': ['Rogers Park', '269605'],
         '4729058': ['Morgan Park', '269595'],
         '4729219': ['West Town', '269615']}
    for i in unmatches.index:
        cha_geoid_zillow.loc[i, ["Name", "RegionID"]] = values[i]        
    return cha_geoid_zillow

def go():
    '''
    process cha data to map each housing unit to geoid and 
    zillow regionid.
    '''
    cha = load_and_clean_cha(CHA_RAW_FILE)
    gcha = convert_to_gdf(cha)
    cha_with_geoid = add_blocks_to_cha(gcha, BLOCKS_FILE)
    cha_geoid_zillow = add_zillow_regionid_to_cha(cha_with_geoid, 
                                                    ZILLOW_SHP_FILE)
    return cha_geoid_zillow
