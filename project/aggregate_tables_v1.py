import pandas as pd
import numpy as np


#delete when done
def read_locator_db(filepath):
    ld = pd.read_csv(filepath, index_col="index")

    return ld


def read_block_group_data(evictions_filepath):
    evictions = pd.read_csv(evictions_filepath)
    evictions_2016 = evictions[evictions['year']==2016]

    race_vars = ['pct-white', 'pct-af-am', 'pct-hispanic', 'pct-asian']
    for var in race_vars:
        new_var = var[4:] + '_pop'
        evictions_2016[new_var] = (evictions_2016[var]/100)*evictions_2016['population']

    evictions_2016 = evictions_2016[['GEOID', 'parent-location', 'population',
                                    'renter-occupied-households',
                                    'median-household-income',
                                    'eviction-filings', 'evictions',
                                    'white_pop', 'af-am_pop', 'hispanic_pop',
                                    'asian_pop']]
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

    merged_w_cha = evictions_full.merge(ld_by_geoid, 
                                        how="left", on="GEOID", indicator=True)

    return merged_w_cha


def master_agg_table_by_neighborhood(chicago_block_df, ld_by_geoid):
    '''
    1. Merge locator database to chicago df by geoid
    2. agg by neighborhood
    3. add column to indicate a "CHA neighborhood vs. non-CHA neighborhood"
        (by threshold)
    4. Add neighborhood level calculations
        - weighted avg for median income by neighborhood
        - we only have the median income by geoid.
        - to create an imperfect calculation of median income of 
          neighborhood:
            - Create a weight for each 
    '''
    agg = ld_aggregated_geoid.groupby('Neighborhood').agg('sum')

    #neighborhood level calculations
    agg['percent_wi_quart_mi_l_stop'] = agg['stop_wi_quart_mi']/agg['num_cha_properties']
    agg['percent_wi_half_mi_l_stop'] = agg['stop_wi_half_mi']/agg['num_cha_properties']
    agg['Avg_monthly_rent'] = agg['total_rent']/agg['num_cha_properties']
    agg['eviction_rate'] = (agg['evictions']/agg['renter-occupied-households'])*100
    agg['eviction_filing_rate'] = (agg['eviction-filings']/agg['renter-occupied-households'])*100    
    agg['perc_rentals_cha'] = agg['num_cha_properties']/agg['renter-occupied-households']

    #weighted avg for median income by neighborhood


def agg_cha_table_by_neighborhood(ld_aggregated_geoid,
                                    output_file):
    #generate transit dummy
    # merged = locator_database_aggregated_geoid.merge(block_group_df, how="left", on="GEOID", indicator=True)
    
    # agg = ld_aggregated_geoid.groupby('Neighborhood').agg('sum')
    # #calculate rates by Neighborhood
    # agg['percent_wi_quart_mi_l_stop'] = agg['stop_wi_quart_mi']/agg['num_cha_properties']
    # agg['percent_wi_half_mi_l_stop'] = agg['stop_wi_half_mi']/agg['num_cha_properties']
    # agg['Avg_monthly_rent'] = agg['total_rent']/agg['num_cha_properties']
    # agg['eviction_rate'] = (agg['evictions']/agg['renter-occupied-households'])*100
    # agg['eviction_filing_rate'] = (agg['eviction-filings']/agg['renter-occupied-households'])*100    
    # agg['perc_rentals_cha'] = agg['num_cha_properties']/agg['renter-occupied-households']
    # # agg = agg.filter(['num_cha_properties',
    # #                   'Avg_monthly_rent',
    # #                   'percent_wi_quart_mi_l_stop',
    # #                   'percent_wi_half_mi_l_stop',
    # #                   'num_props_w_potential_bad_landlord',
    # #                   'eviction_rate', 
    # #                   'eviction_filing_rate',
    # #                   'population'], axis=1)

    
    agg = agg.sort_values(by='num_cha_properties', ascending=False)
    
    ld_by_neighborhood = agg[['num_cha_properties', 'Avg_monthly_rent', 
               'stop_wi_quart_mi',  'stop_wi_half_mi', 
               'percent_wi_quart_mi_l_stop', 'percent_wi_half_mi_l_stop',
               'eviction-filings', 'evictions',
               'eviction_filing_rate', 'eviction_rate',
               'renter-occupied-households', 'perc_rentals_cha',
               'population']]

    ld_by_neighborhood.to_csv(output_file)

    return ld_by_neighborhood

  

# def agg_CHA_non_CHA_compare(ld_by_neighborhood, block_group_df, output_file):
    

#     grouped = block_group_df.groupby('name')
#     grouped.agg({'population': 'sum',
#         'renter-occupied-households'})


#     # merged = block_group_df.merge(locator_database_aggregated_geoid,
#     #                               how="left", on="GEOID", indicator=True)

#     merged['cha_neighborhood'] = np.where(merged['_merge'] == "both", 1, 0)

#     race_vars = ['pct-white', 'pct-af-am', 'pct-hispanic', 'pct-am-ind', 'pct-asian']
#     for var in race_vars:
#         new_var = var[4:] + '_pop'
#         merged[new_var] = (merged[var]/100)*merged['population']

#     #weight the geoids by the population 
#     merged['geoid_weight'] = merged['population']/merged['population'].sum()
    
#     merged['median-household-income-wght'] = merged['median-household-income']*merged['geoid_weight']

#     grouped = merged.groupby('cha_neighborhood')

#     agg = grouped.agg({'num_cha_properties' : 'sum', 
#                        'population' : 'sum',
#                        'evictions' : 'sum',
#                        'eviction-filings' : 'sum',
#                        'renter-occupied-households' : 'sum',
#                        'white_pop' : 'sum',
#                        'af-am_pop' : 'sum',
#                        'hispanic_pop' : 'sum',
#                        'asian_pop' : 'sum',
#                        'median-household-income-wght' : 'sum'
#                     })

#     #calculate rates
#     agg['eviction_rate'] = agg['evictions']/agg['renter-occupied-households']
#     agg['eviction_filing_rate'] = agg['eviction-filings']/agg['renter-occupied-households']
#     agg['white-pct'] = agg['white_pop']/agg["population"]
#     agg['af-am-pct'] = agg['af-am_pop']/agg["population"]
#     agg['hispanic-pct'] = agg['hispanic_pop']/agg["population"]

#     agg.to_csv(output_file)

#     return agg

