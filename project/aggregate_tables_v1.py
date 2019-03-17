import pandas as pd
import numpy as np


#delete when done
def read_locator_db(filepath):
    ld = pd.read_csv(filepath, index_col="index")

    return ld


def read_block_group_data(block_groups_csv):
    evictions = pd.read_csv(block_groups_csv)
    evictions_2016 = evictions[evictions['year']==2016]

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

'''
make a cook county comparison for eviction rate data
'''
def aggregate_cha_by_geoid(locator_database):
    '''
    aggregate the locator_database by geoid. This will be the dataset
    used to merge with the block group data for both the following aggregate
    tables.
    '''
    locator_database['stop_wi_quart_mi'] = np.where(locator_database['num_stops_quart_mi'] > 0, 1, 0)
    locator_database['stop_wi_half_mi'] = np.where(locator_database['num_stops_half_mi'] > 0, 1, 0)
    locator_database['pot_bad_landlord'] = np.where(locator_database['potential_bad_landlord'] == True, 1, 0)

    group_geoids = locator_database.groupby(["GEOID"]) 
    ld_by_geoid = group_geoids.agg({'Address' : 'count', 
                                   'stop_wi_quart_mi' : 'sum', 
                                   'stop_wi_half_mi' : 'sum', 
                                   'pot_bad_landlord' : 'sum',
                                   'Monthly Rent' : 'sum', 
                                    }) 

    ld_by_geoid = ld_by_geoid.rename(columns={'Address': "num_cha_properties", 
                              'pot_bad_landlord': "num_props_w_potential_bad_landlord",
                              'Monthly Rent': 'total_rent'
                               })

    ld_by_geoid = ld_by_geoid.reset_index()

    return ld_by_geoid

def make_master_agg_dataset(list_chicago_geoids_csv, block_group_df, 
                                geoid_to_region_id_map_csv, ld_by_geoid):
    '''
    1.  read in list of geoids in chicago
    2. read in list of geoids mapped to regions ids and neighborhoods
    3. merge the geoids-regionid df (right) to list of geoids in chicago (right)
        to have geoids and region ids in the city of chicago
    4. merge the full evictions dataset (block_group_df) with the df above
    5. 
    chicago geoids: "list_of_chi_geoids.csv"
    geoid_to_region_id_map_csv: "all_geography_merge.csv"
    '''
    chi_geoid = pd.read_csv(list_chicago_geoids_csv, 
                            header=None, names=["index", "GEOID"], usecols=[1])
    chi_geoid = chi_geoid.drop_duplicates('GEOID', keep='first')

    geoid_neigh = pd.read_csv(geoid_to_region_id_map_csv, usecols=[1,2,3])
    geoid_neigh = geoid_neigh.drop_duplicates('GEOID', keep='first')

    chi_geoid_neigh = chi_geoid.merge(geoid_neigh, how="left", on="GEOID")
    evictions_full = chi_geoid_neigh.merge(block_group_df, 
                                           how="left", on="GEOID")
    evictions_full = evictions_full.rename(columns={'Name': 'Neighborhood'})

    agg_df_by_geoid = evictions_full.merge(ld_by_geoid, 
                                        how="left", on="GEOID", indicator=True)

    return agg_df_by_geoid


def master_agg_by_neighborhood(agg_df_by_geoid, output_file):
    '''
    Create first aggregation table of the information about neighborhoods
    continaing CHA houses
    '''
    agg = agg_df_by_geoid.groupby('Neighborhood').agg('sum')

    #neighborhood level calculations

    #cha related stats
    agg['percent_wi_quart_mi_l_stop'] =agg['stop_wi_quart_mi']/agg['num_cha_properties']
    agg['percent_wi_half_mi_l_stop'] = agg['stop_wi_half_mi']/agg['num_cha_properties']
    agg['Avg_cha_monthly_rent'] = agg['total_rent']/agg['num_cha_properties']
    agg['perc_rentals_hcv'] = agg['num_cha_properties']/agg['renter-occupied-households']
    agg['perc_hcv_in_neigh'] = agg['num_cha_properties']/agg['num_cha_properties'].sum()
    agg['at_least_10_homes'] = np.where(agg['num_cha_properties'] >= 10, 1, 0)

    #eviction and poverty rates
    agg['eviction_rate'] = (agg['evictions']/agg['renter-occupied-households'])
    agg['eviction_filing_rate'] = (agg['eviction-filings']/agg['renter-occupied-households'])   
    agg['poverty_rate'] = agg['people_in_poverty']/agg['population']
    agg['evictions_chi_percentile_rank']=(agg['eviction_rate'].rank(pct=True))
    
    #demographic rates
    agg['perc_black'] = agg['af-am_pop']/agg['population']
    agg['perc_latino'] = agg['hispanic_pop']/agg['population']
    agg['perc_white'] = agg['white_pop']/agg['population']

    agg = agg.sort_values(by='num_cha_properties', ascending=False)
    
    # neigh_w_chas = agg[agg['num_cha_properties'] > 0]
    master_agg_by_neighborhood = agg[['num_cha_properties', 'Avg_cha_monthly_rent', 
               'stop_wi_quart_mi',  'stop_wi_half_mi', 
               'percent_wi_quart_mi_l_stop', 'percent_wi_half_mi_l_stop', 
               "num_props_w_potential_bad_landlord",
               'poverty_rate',
               'eviction-filings', 'evictions',
               'eviction_filing_rate', 'eviction_rate', 'evictions_chi_percentile_rank',
               'renter-occupied-households', 'perc_rentals_hcv',
               'population',  'perc_hcv_in_neigh', 'at_least_10_homes', 
               'perc_black','perc_latino', 'perc_white' ]]

    master_agg_by_neighborhood.to_csv(output_file)

    return master_agg_by_neighborhood

#decide which columns to keep or drop if you want to? 

## meaningful question - What choices to HCV families have? Where can they live?

def agg_CHA_non_CHA_compare(master_agg_by_neigh, output_file):
    '''
    Compare neighborhoods with at least 10 homes available in them to homes
    with less than 10. 96% of the HCV homes in the list are in neighborhoods 
    with at least 10 homes available.

    https://interactive.wbez.org/curiouscity/segregation-map/ 

    Note: Used WBEZ's defintion of integrated from source above: 
        Integrated = "Which we define as a condition where no single race 
        comprises two-thirds or more of an area’s population.
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

    return neigh_summary

def num_homes_in_mobility_neigh(master_agg_by_neigh, output_file):
    '''
    % of homes in neighborhoods with less than 20% poverty
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

    return neigh_by_pov

#reorder
#perhaps make them into percentages? 
#make pretty in excel.
def build_agg_tables(locator_database_csv, block_groups_csv, 
       list_chicago_geoids_csv, geoid_to_region_id_map_csv,
       output_file1, output_file2, output_file3):

    locator_database = pd.read_csv(locator_database_csv, index_col="index")
    block_group_df = read_block_group_data(block_groups_csv)
    ld_by_geoid = aggregate_cha_by_geoid(locator_database)
    agg_df_by_geoid = make_master_agg_dataset(list_chicago_geoids_csv, 
                                             block_group_df, 
                                             geoid_to_region_id_map_csv, 
                                             ld_by_geoid)

    master_agg_by_neigh = master_agg_by_neighborhood(agg_df_by_geoid, output_file1)
    agg_CHA_non_CHA = agg_CHA_non_CHA_compare(master_agg_by_neigh, output_file2)
    homes_by_poverty = num_homes_in_mobility_neigh(master_agg_by_neigh, output_file3)


LOCATOR_DB_CSV = "processed_data/locator_database.csv"
BLOCK_GROUPS_CSV = "data/block-groups.csv" 
CHICAGO_GEOIDS = "list_of_chi_geoids.csv"
GEOID_TO_REGIONID = "all_geography_merge.csv"
OUTPUT_FILE1 = "processed_data/agg_table_by_neighborhood.csv"
OUTPUT_FILE2 = "processed_data/agg_table_hsv_non_hsv.csv"
OUTPUT_FILE3 = "processed_data/homes_in_pov_neighborhood.csv"


def main():
    '''
    Build, format, and stores database as csv.
    Returns nothing.
    '''
    db = build_agg_tables(
        LOCATOR_DB_CSV, 
        BLOCK_GROUPS_CSV, 
        CHICAGO_GEOIDS, 
        GEOID_TO_REGIONID,
        OUTPUT_FILE1,
        OUTPUT_FILE2,
        OUTPUT_FILE3)


if __name__ == '__main__':
    main()