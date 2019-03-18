'''
Generates three csvs with neighborhood level summary statistics and a map

Aya Liu, Bhargavi Ganesh, Vedika Ahuja

'''
import sys
import pandas as pd
import numpy as np
import geopandas
import matplotlib.pyplot as plt

LOCATOR_DB_CSV = "processed_data/locator_database.csv"
BLOCK_GROUPS_CSV = "data/block-groups.csv" 
CITY_BLOCKS_JSON = "data/Boundaries-CensusBlocks-2010.geojson"
ZILLOW_SHAPEFILE = "data/ZillowNeighborhoods-IL.shp"

#turn off user warnings
pd.options.mode.chained_assignment = None
import warnings
warnings.filterwarnings("ignore")

def build_agg_tables(locator_database_csv, block_groups_csv, city_blocks_json, 
                     zillow_shapefile, output_file1, output_file2, output_file3, 
                     map_filename):
    '''
    Builds aggregate tables and map using functions below.

    Inputs:
        -locator_database_csv: (csv file) of the final locator database 
        -block_groups_csv: (csv file) of the evictions database
        -city_blocks_json: (geojson file) of the city of Chicago by block group
        -zillow_shapefile: (shapefile) of the state of Illinois by 
                            zillow-defined neighborhood
        -output_file1: filename (specified by the user) with 
                        a ".csv" ending for the CHA neighborhood-level tables
        -output_file2: filename (specified by the user) with a ".csv" 
                       ending for table comparing CHA and non-CHA properties
        -output_file3: filename (specified by the user) with a ".csv" ending 
                       for aggregate table comparing CHA and non-CHA properties
        -map_filename: filename (specified by the user) with a ".png" ending for 
                       output map showing the number of CHA properties on a map
    '''
    #Inputs
    locator_database = read_locator_db(locator_database_csv)
    block_group_df = read_block_group_data(block_groups_csv)
    ld_by_geoid = aggregate_cha_by_geoid(locator_database)
    blocks_zillow_merge = create_city_blocks(city_blocks_json, zillow_shapefile)
    agg_df_by_geoid = make_master_agg_dataset(
        blocks_zillow_merge, block_group_df, ld_by_geoid)

    #Output files
    master_agg_by_neigh = master_agg_by_neighborhood(
        agg_df_by_geoid, output_file1)
    agg_CHA_non_CHA = agg_CHA_non_CHA_compare(master_agg_by_neigh, output_file2)
    homes_by_poverty = num_homes_in_mobility_neigh(
        master_agg_by_neigh, output_file3)
    map_of_cha_properties = generate_map(
        locator_database, blocks_zillow_merge, map_filename)


def read_locator_db(filepath):
    '''
    Reads in the locator database using the specified filepath 
    and returns a pandas dataframe.

    Inputs:
        -filepath: filepath of locator database

    Returns:
        -ld: pandas dataframe of locator database
    '''
    col_types = {
    'index': str,
    'Address': str,
    'Monthly Rent': 'int64',
    'Propterty Type': str,
    'Bath': float,
    'Bed': float,
    'Availability': str,
    'Contact': str,
    'URL': str,
    'Lat': float,
    'Long': float,
    'Coordinates': str,
    'GEOID': str,
    'State': str,
    'County': str,
    'City': str,
    'Neighborhood': str,
    'RegionID': str,
    'parent-location': str,
    'population': float,
    'renter-occupied-households': float,
    'median-gross-rent': float,
    'median-household-income': float,
    'median-property-value': float,
    'pct-white': float,
    'pct-af-am': float,
    'pct-hispanic': float,
    'pct-asian': float,
    'eviction-filings': float,
    'evictions': float,
    '2016_evict_rate': float,
    '2016_evict_filing_rate': float,
    '2011-2015_rent_perc_change': float,
    '2015-2019_rent_perc_change': float,
    'potential_bad_landlord': bool,
    'bad_landlord_address': str,
    'num_stops_quart_mi': int,
    'num_stops_half_mi': int,
    'num_stops_3quart_mi': int,
    'num_stops_1_mi': int,
    'er_percentile': float,
    'efr_percentile': float,
    'stop_wi_quart_mi': int,
    'stop_wi_half_mi': int,
    'pot_bad_landlord': int
    }

    ld = pd.read_csv(filepath, index_col="index", dtype=col_types)

    return ld


def read_block_group_data(block_groups_csv):
    '''
    Reads in the eviction database (for all Census block groups in Illinois) 
    using the specified filepath and returns a pandas dataframe for 2016.

    Inputs:
        -block_groups_csv: filepath of eviction database

    Returns:
        -evictions_2016: pandas df of evictions for IL blockgroups in 2016

    '''
    col_types = {
    'GEOID': str,
    'year': str,
    'name': str,
    'parent-location': str,
    'population': float,
    'poverty-rate': float,
    'renter-occupied-households': float,
    'pct-renter-occupied': float,
    'median-gross-rent': float,
    'median-household-income': float,
    'median-property-value': float,
    'rent-burden': float,
    'pct-white': float,
    'pct-af-am': float,
    'pct-hispanic': float,
    'pct-am-ind': float,
    'pct-asian': float,
    'pct-nh-pi': float,
    'pct-multiple': float,
    'pct-other': float,
    'eviction-filings': float,
    'evictions': float,
    'eviction-rate': float,
    'eviction-filing-rate': float,
    'low-flag': int,
    'imputed': int,
    'subbed': int
    }

    evictions = pd.read_csv(block_groups_csv, dtype=col_types)
    evictions_2016 = evictions[evictions['year']=='2016']
    race_vars = ['pct-white', 'pct-af-am', 'pct-hispanic', 'pct-asian']
    for var in race_vars:
        new_var = var[4:] + '_pop'
        evictions_2016[new_var] = (evictions_2016[var]/100)*evictions_2016['population']

    evictions_2016['people_in_poverty'] = evictions_2016['population']* \
                            (evictions_2016['poverty-rate']/100)
    evictions_2016 = evictions_2016[['GEOID', 'parent-location', 'population',
                                    'renter-occupied-households',
                                    'median-household-income',
                                    'eviction-filings', 'evictions',
                                    'white_pop', 'af-am_pop', 'hispanic_pop',
                                    'asian_pop', 'people_in_poverty']]

    return evictions_2016


def aggregate_cha_by_geoid(locator_database):
    '''
    Aggregates the locator_database by geoid. This will be the dataset
    used to merge with the block group data for the aggregate tables.

    Inputs:
        -locator_database(pandas dataframe) of locator database read in using 
         read_locator_db

    Returns:
        -ld_by_geoid: pandas dataframe of locator database aggregated by geoid
    '''
    locator_database['stop_wi_quart_mi'] = np.where(
        locator_database['num_stops_quart_mi'] > 0, 1, 0)
    locator_database['stop_wi_half_mi'] = np.where(
        locator_database['num_stops_half_mi'] > 0, 1, 0)
    locator_database['pot_bad_landlord'] = np.where(
        locator_database['potential_bad_landlord'] == True, 1, 0)

    group_geoids = locator_database.groupby(["GEOID"]) 
    ld_by_geoid = \
    group_geoids.agg({
    'Address' : 'count', 
    'stop_wi_quart_mi' : 'sum', 
    'stop_wi_half_mi' : 'sum', 
    'pot_bad_landlord' : 'sum',
    'Monthly Rent' : 'sum', 
    }) 

    ld_by_geoid = \
    ld_by_geoid.rename(columns={
    'Address': "num_cha_properties", 
    'pot_bad_landlord': "num_props_w_potential_bad_landlord",
    'Monthly Rent': 'total_rent'
    })

    ld_by_geoid = ld_by_geoid.reset_index()

    return ld_by_geoid


def create_city_blocks(city_blocks_json, zillow_shapefile):
    '''
    Generates merged dataframe (using geopandas) of Census block groups mapped
    to zillow neighborhoods for the city of Chicago. 

    Inputs:
        -city_blocks_json: geojson of Census block groups clipped for Chicago

    Returns:
        -blocks_zillow_merge: geopandas df of Census block groups mapped to 
         zillow neighborhood
    '''
    city_blocks_df = geopandas.read_file(city_blocks_json)
    city_blocks_df['GEOID'] = \
    city_blocks_df['geoid10'].str.slice(start=0, stop=12)
    # set coordinate reference systems
    zillow_neighborhoods = geopandas.read_file(zillow_shapefile)
    city_blocks_df.crs = {'init' :'epsg:4326'}
    zillow_neighborhoods.crs = {'init' :'epsg:4326'}
    block_zillow_merge = geopandas.sjoin(
        city_blocks_df, zillow_neighborhoods, how="left", op="intersects")

    return block_zillow_merge


def make_master_agg_dataset(blocks_zillow_merge, block_group_df, ld_by_geoid):
    '''
    Creates dataframe aggregated by geoid of evictions data (for the city of Chicago)
    merged with locator database of CHA properties.

    Inputs: 
        -blocks_zillow_merge: (geopandas) dataframe of zillow neighborhoods mapped to 
                               Census block groups
        -block_group_df: (pandas) dataframe of Census block-group level evictions data
        -ld_by_geoid: (pandas) dataframe of locator database aggregated by geoid

    Returns:
        -agg_df_by_geoid: master aggregated table used to make summary tables below
    '''
    df_to_merge = blocks_zillow_merge[['GEOID', 'Name', 'RegionID']]
    df_to_merge = df_to_merge.drop_duplicates('GEOID', keep="first")
    evictions_full = pd.merge(df_to_merge, block_group_df, how="left", on="GEOID")
    evictions_full = evictions_full.rename(columns={'Name': 'Neighborhood'})
    agg_df_by_geoid = evictions_full.merge(
        ld_by_geoid, how="left", on="GEOID", indicator=True)

    return agg_df_by_geoid


def master_agg_by_neighborhood(agg_df_by_geoid, output_file):
    '''
    Creates first aggregation table of information about neighborhoods
    containing CHA houses

    Inputs:
        -agg_df_by_geoid: (pandas df) master aggregated table of evictions data and locator database
                          for city of Chicago, aggregated by geoid
        -output_file: (str) output csv path for neighborhood-level aggregated table
    Returns:
        -master_agg_by_neighborhood: table aggregated by zillow neighborhood
    '''
    agg = agg_df_by_geoid.groupby('Neighborhood').agg('sum')

    #neighborhood level calculations
    #cha related stats
    agg['percent_wi_quart_mi_l_stop'] = agg['stop_wi_quart_mi'] / agg['num_cha_properties']
    agg['percent_wi_half_mi_l_stop'] = agg['stop_wi_half_mi'] / agg['num_cha_properties']
    agg['Avg_cha_monthly_rent'] = agg['total_rent'] / agg['num_cha_properties']
    agg['perc_rentals_hcv'] = agg['num_cha_properties'] / agg['renter-occupied-households']
    agg['perc_hcv_in_neigh'] = agg['num_cha_properties'] / agg['num_cha_properties'].sum()
    agg['at_least_10_homes'] = np.where(agg['num_cha_properties'] >= 10, 1, 0)

    #eviction and poverty rates
    agg['eviction_rate'] = (agg['evictions'] / agg['renter-occupied-households'])
    agg['eviction_filing_rate'] = (agg['eviction-filings'] / agg['renter-occupied-households'])   
    agg['poverty_rate'] = agg['people_in_poverty'] / agg['population']
    agg['evictions_chi_percentile_rank'] = (agg['eviction_rate'].rank(pct=True))
    
    #demographic rates
    agg['perc_black'] = agg['af-am_pop'] / agg['population']
    agg['perc_latino'] = agg['hispanic_pop'] / agg['population']
    agg['perc_white'] = agg['white_pop'] / agg['population'] 
    agg = agg.sort_values(by='num_cha_properties', ascending=False)
    
    master_agg_by_neighborhood = \
    agg[['num_cha_properties', 'Avg_cha_monthly_rent', 
         'stop_wi_quart_mi',  'stop_wi_half_mi', 
         'percent_wi_quart_mi_l_stop', 'percent_wi_half_mi_l_stop', 
         'num_props_w_potential_bad_landlord','poverty_rate',
         'eviction-filings', 'evictions',
         'eviction_filing_rate', 'eviction_rate', 
         'evictions_chi_percentile_rank','renter-occupied-households', 
         'perc_rentals_hcv','population',  'perc_hcv_in_neigh', 
         'at_least_10_homes', 'perc_black','perc_latino', 'perc_white']]

    master_agg_by_neighborhood.to_csv(output_file)

    return master_agg_by_neighborhood


def agg_CHA_non_CHA_compare(master_agg_by_neigh, output_file):
    '''
    Creates second aggregation table comparing neighborhoods with at 
    least 10 homes available in them to homes with less than 10. 
    96% of the HCV homes in the list are in neighborhoods with at least 10 homes 
    available.

    https://interactive.wbez.org/curiouscity/segregation-map/ 

    Note: Used WBEZ's defintion of integrated from source above: 
        Integrated = "Which we define as a condition where no single race 
        comprises two-thirds or more of an area’s population.

    Inputs:
        -master_agg_by_neigh (pandas dataframe): master aggregated table of 
         evictions data and locator database for Chicago, aggregated by 
         zillow neighborhood
        -output_file: (str) output csv path for CHA vs non-CHA aggregated table
    Returns:
        -neigh_summary (pandas df): summary table by zillow neighborhood
    '''
    master_agg_by_neigh['less_20_perc_pov'] = np.where(master_agg_by_neigh['poverty_rate'] < .2, 
                                                        1, 0)
    master_agg_by_neigh['maj_black'] = np.where(master_agg_by_neigh['perc_black'] >=.66, 1, 0)
    master_agg_by_neigh['maj_white'] = np.where(master_agg_by_neigh['perc_white'] >=.66, 1, 0)
    master_agg_by_neigh['maj_latino'] = np.where(master_agg_by_neigh['perc_latino'] >=.66, 1, 0)
    integrated_mask = (master_agg_by_neigh['maj_black'] == 0) & \
                      (master_agg_by_neigh['maj_white'] == 0) & \
                      (master_agg_by_neigh['maj_latino'] == 0)                  
    master_agg_by_neigh['integrated'] = np.where(integrated_mask == True, 1, 0)
    grouped =  master_agg_by_neigh.groupby('at_least_10_homes')
    agg = grouped.agg({'num_cha_properties' : 'count',
                       'eviction_rate' : 'mean',
                       'poverty_rate' : 'mean',
                       'less_20_perc_pov' : 'sum',
                       'maj_black' : 'sum',
                       'maj_white' : 'sum',
                       'maj_latino' : 'sum',
                       'integrated' : 'sum'})  
    agg = agg.rename(columns={'num_cha_properties' : 'num_neighborhoods'})
    neigh_summary = agg[['num_neighborhoods','poverty_rate', 'less_20_perc_pov', 
                         'eviction_rate', 'integrated',  'maj_black', 'maj_white',
                         'maj_latino']]

    neigh_summary.to_csv(output_file)


def num_homes_in_mobility_neigh(master_agg_by_neigh, output_file):
    '''
    Creates final aggregation table of % of homes in neighborhoods with less 
    than 20% poverty.

    Inputs:
        -master_agg_by_neigh: (pandas dataframe): master aggregated table of evictions data and 
         locator database for city of Chicago, aggregated by zillow neighborhood
        -output_file: (str) output csv path for neighborhood-level poverty table

    '''
    master_agg_by_neigh['less_20_perc_pov'] = np.where(master_agg_by_neigh['poverty_rate'] < .2, 
                                                        1, 0)
    grouped =  master_agg_by_neigh.groupby('less_20_perc_pov')
    agg = grouped.agg({'num_cha_properties' : 'sum',
                        'eviction_rate' : 'mean',
                        'poverty_rate' : 'mean'})

    neigh_by_pov = agg[['num_cha_properties','poverty_rate', 
                        'eviction_rate']]
    neigh_by_pov.to_csv(output_file)


def generate_map(locator_database, blocks_zillow_merge, map_filename):
    '''
    Creates chloropleth map visualizing number of properties by Census blockgroup.

    Inputs:
        -locator_database:(pandas dataframe) of locator database read in using 
         read_locator_db
        -blocks_zillow_merge: geopandas dataframe of Census block groups mapped to zillow
         neighborhoods
        -map_filename: (str) output png path for map
    '''
    #preparing data for map
    count_series = locator_database.groupby('GEOID').size()
    new_df = pd.DataFrame()
    new_df['GEOID'] = count_series.index
    city_blocks = blocks_zillow_merge[['statefp10', 'name10', 'blockce10', 
                                       'tract_bloc', 'geoid10', 'tractce10', 
                                       'countyfp10', 'geometry', 'GEOID']]
    new_df['count_properties'] = count_series.values
    merged = pd.merge(city_blocks, new_df, how="left")
    merged = merged.fillna(0)

    #mapping
    variable = 'count_properties'
    vmin, vmax = 0, 50
    fig, ax = plt.subplots(1, figsize=(8, 6))
    merged.plot(column=variable, cmap='Blues', scheme='Quantiles', ax=ax, alpha=1)
    ax.axis('off')
    sm = plt.cm.ScalarMappable(
        cmap='Blues', norm=plt.Normalize(vmin=vmin, vmax=vmax))
    sm._A = []
    cbar = fig.colorbar(sm)
    ax.set_title('Number of CHA properties by Census Block Group', 
                 fontdict={'fontsize': '15', 'fontweight' : '3'})
    fig.savefig(map_filename, dpi=300)


def main(output1, output2, output3, map_filename):
    '''
    Builds, formats, and stores database as csv.
    Returns nothing.
    '''
    db = build_agg_tables(
        LOCATOR_DB_CSV, 
        BLOCK_GROUPS_CSV, 
        CITY_BLOCKS_JSON,   
        ZILLOW_SHAPEFILE, 
        output1,
        output2,
        output3,
        map_filename)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])