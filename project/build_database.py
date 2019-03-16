'''
Create an affordable rental unit database from the following data sources:

- CHA HCV rental unit listings
- Eviction Lab
- Zillow Rent Index
- CTA L-stops
- Problem Landlords

'''
import pandas as pd
from process_cha_data import process_cha_data
import rent_and_eviction as rev
import transit_and_landlord as trl
pd.options.display.max_columns = 999

# data source filepaths
CHA_DATA = "data/CHA_rental_data.obj"
ZILLOW_DATA = "data/Neighborhood_Zri_AllHomesPlusMultifamily.csv"
EVICTIONS_DATA = "data/block-groups.csv"
L_STOPS_DATA = "data/CTA_L_stops_locations.csv"
BAD_LL_DATA = "data/problem_landlords.csv"
BLOCKS_GEOFILE = "data/block-groups.geojson"
ZILLOW_GEOFILE = "data/ZillowNeighborhoods-IL.shp"

# output data filepaths
ZILLOW_WITH_INC = "processed_data/zillow_rindex_with_increase.csv"
DATABASE = "processed_data/locator_database.csv"

# default threshold for landlord location fuzzy match
DEF_TS = 0.015


def build_database(cha_data, evictions_data, zillow_data, lstops_data,
            bad_landlords_data, blocks_geofile, zillow_geofile, 
            zillow_with_inc_output, database_output, threshold=DEF_TS):
    '''
    Merge all data sources and write the merged dataset to a csv file

    Inputs:
        Filepaths to data sources:
            - cha_data: filename of CHA rental unit data (raw)
            - evictions_data: filename of the EvictionLab dataset
            - zillow_data: filename of the Zillow Rent Index dataset
            - lstops_data
            - bad_landlords_data
            - blocks_geofile: filename of the Census block group geojson
            - zillow_geofile: filename of the Zillow Regions shp file

        Filepaths to output csv files:
            - zillow_with_inc_output: output filename for zillow data with 
                                        computed rent increase rate
            - database_output: output filename for database created

    Returns: (GeoDataFrame) merged dataset mapping rental units to
        - 2016 eviction rate and eviction filing rate (block-group level)
        - 2011-2015 and 2015-2019 rent increase rates (neighborhood level)
        - numbers of L-stations within .25, .5, .75 and 1 mile (unit level)
        - flags for problem landlords (unit level)

    '''
    print("Processing housing data sources...")
    # process data of housing units, eviction rates, and rent index
    cha = process_cha_data(cha_data, blocks_geofile, zillow_geofile)
    evict = rev.read_and_process_evictions(evictions_data)
    rindex = rev.read_and_process_rindex(zillow_data, 
                                            zillow_with_inc_output)

    print("Flagging problem landlords...")
    # flag units with potential bad landlords
    cha_to_landlords = trl.flag_potential_bad_landlord(cha,
                            bad_landlords_data, threshold)

    print("Computing transit access...")
    # compute transit access for each unit
    cha_to_transit = trl.compute_num_stations(cha, lstops_data)

    print("Building the database...")
    # merge all processed data sources above
    merged = merge_with_evict(cha, evict)
    merged = merge_with_rindex(merged, rindex)
    merged = merge_on_index(merged, cha_to_landlords)
    merged = merge_on_index(merged, cha_to_transit)

    print("Saving the database...")
    # format and write to csv
    format_db(merged)
    merged.to_csv(database_output)

    print("--Finished--")
    return merged        


def merge_with_evict(cha, evict):
    evict_to_merge = evict[evict.year == '2016']
    evict_to_merge = evict_to_merge[['GEOID', 'eviction-rate', 
            'eviction-filing-rate', 'parent-location', 'population',
            'renter-occupied-households', 'median-gross-rent',
            'median-household-income', 'median-property-value', 'pct-white', 
            'pct-af-am', 'pct-hispanic', 'pct-am-ind', 'pct-asian', 
            'eviction-filings', 'evictions', 'eviction-rate',
            'eviction-filing-rate']]
    merged_with_ev = pd.merge(cha.reset_index(), evict_to_merge, on="GEOID", 
                            how="left")
    return merged_with_ev


def merge_with_rindex(cha, rindex):
    rindex_to_merge = rindex[["RegionID","2011-2015", "2015-2019"]]
    merged_with_rindex = pd.merge(cha, rindex_to_merge, on="RegionID", 
                            how="left")
    return merged_with_rindex


def merge_on_index(df, df_to_merge):
    df_to_merge.rename(columns={"ind_apt": "index"}, inplace=True)
    merged = pd.merge(df, df_to_merge, on="index", how="left")
    return merged


def format_db(df):
    df.set_index("index", inplace=True)
    new_names = {
    "Name": "Neighborhood",
    "eviction-rate": "2016_evict_rate",
    "eviction-filing-rate": "2016_evict_filing_rate",
    "2011-2015": "2011-2015_rent_perc_change",
    "2015-2019": "2015-2019_rent_perc_change",
    "Address_ll": "bad_landlord_address",
    "wi_quart_mi": "num_stops_quart_mi",
    "wi_half_mi": "num_stops_half_mi",
    "wi_3_quart": "num_stops_3quart_mi",
    "wi_1_mi": "num_stops_1_mi",    
              }
    df.rename(columns=new_names, inplace=True)


def main():
    '''
    Build, format, and stores database as csv.
    Returns nothing.
    '''
    db = build_database(
        CHA_DATA, 
        EVICTIONS_DATA, 
        ZILLOW_DATA, 
        L_STOPS_DATA,
        BAD_LL_DATA, 
        BLOCKS_GEOFILE, 
        ZILLOW_GEOFILE, 
        ZILLOW_WITH_INC,
        DATABASE
        )


if __name__ == '__main__':
    main()