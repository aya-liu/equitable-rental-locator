import pandas as pd
import numpy as np


#delete when done
def read_locator_db(filepath):
    ld = pd.read_csv(filepath, index_col="index")

    return ld


def read_block_group_data(evictions_filepath):
    evictions = pd.read_csv(evictions_filepath)
    evictions_2016 = evictions[evictions['year']==2016]

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

    group_geoids = locator_database.groupby(["GEOID", "Name"]) 

    agg = group_geoids.agg({'Address' : 'count', 
                       'stop_wi_quart_mi' : 'sum', 
                       'stop_wi_half_mi' : 'sum', 
                       'pot_bad_landlord' : 'sum',
                       'Monthly Rent' : 'sum' #will have to recalculate average monthly rent at the end
                    }) 

    agg = agg.rename(columns={'Address': "num_cha_properties", 
                              'pot_bad_landlord': "num_props_w_potential_bad_landlord",
                              'Monthly Rent': 'total_rent'
                                })
    agg = agg.reset_index()

    return agg

def aggregate_table_by_neighborhood(locator_database_aggregated_geoid,
                                    block_group_df, output_file):
    #generate transit dummy
    merged = locator_database_aggregated_geoid.merge(block_group_df, how="left", on="GEOID", indicator=True)
    grouped = merged.groupby('Name')
    agg = grouped.agg({'num_cha_properties' : 'sum', 
                       'stop_wi_quart_mi' : 'sum', 
                       'stop_wi_half_mi' : 'sum', 
                       'population' : 'sum',
                       'evictions' : 'sum',
                       'eviction-filings' : 'sum',
                       'renter-occupied-households' : 'sum',
                       "num_props_w_potential_bad_landlord" : 'sum',
                       'total_rent' : 'sum'
                    })


    #calculate rates by Neighborhood
    agg['percent_wi_quart_mi_l_stop'] = agg['stop_wi_quart_mi']/agg['num_cha_properties']
    agg['percent_wi_half_mi_l_stop'] = agg['stop_wi_half_mi']/agg['num_cha_properties']
    agg['Avg_monthly_rent'] = agg['total_rent']/agg['num_cha_properties']
    agg['eviction_rate'] = (agg['evictions']/agg['renter-occupied-households'])*100
    agg['eviction_filing_rate'] = (agg['eviction-filings']/agg['renter-occupied-households'])*100    

    # agg = agg.filter(['num_cha_properties',
    #                   'Avg_monthly_rent',
    #                   'percent_wi_quart_mi_l_stop',
    #                   'percent_wi_half_mi_l_stop',
    #                   'num_props_w_potential_bad_landlord',
    #                   'eviction_rate', 
    #                   'eviction_filing_rate',
    #                   'population'], axis=1)

    agg = agg.sort_values(by='num_cha_properties', ascending=False)

    agg.to_csv(output_file)

    return agg

def filter_chicago_block_groups(list_chicago_geoids_csv, block_group_df):
    '''
    chicago geoids: "list_of_chi_geoids.csv"
    '''
    chi_geoid = pd.read_csv(list_chicago_geoids_csv, 
                            header=None, names=["index", "GEOID"], usecols=[1])
    merged = chi_geoid.merge(block_group_df,
                                  how="left", on="GEOID", indicator=True)

    return merged

def aggregate_table_CHA_compare(locator_database_aggregated_geoid,
                    block_group_df, output_file):
    
    merged = block_group_df.merge(locator_database_aggregated_geoid,
                                  how="left", on="GEOID", indicator=True)

    merged['cha_neighborhood'] = np.where(merged['_merge'] == "both", 1, 0)

    race_vars = ['pct-white', 'pct-af-am', 'pct-hispanic', 'pct-am-ind', 'pct-asian']
    for var in race_vars:
        new_var = var[4:] + '_pop'
        merged[new_var] = (merged[var]/100)*merged['population']

    #weight the geoids by the population 
    merged['geoid_weight'] = merged['population']/merged['population'].sum()
    
    merged['median-household-income-wght'] = merged['median-household-income']*merged['geoid_weight']

    grouped = merged.groupby('cha_neighborhood')

    agg = grouped.agg({'num_cha_properties' : 'sum', 
                       'population' : 'sum',
                       'evictions' : 'sum',
                       'eviction-filings' : 'sum',
                       'renter-occupied-households' : 'sum',
                       'white_pop' : 'sum',
                       'af-am_pop' : 'sum',
                       'hispanic_pop' : 'sum',
                       'asian_pop' : 'sum',
                       'median-household-income-wght' : 'sum'
                    })

    #calculate rates
    agg['eviction_rate'] = agg['evictions']/agg['renter-occupied-households']
    agg['eviction_filing_rate'] = agg['eviction-filings']/agg['renter-occupied-households']
    agg['white-pct'] = agg['white_pop']/agg["population"]
    agg['af-am-pct'] = agg['af-am_pop']/agg["population"]
    agg['hispanic-pct'] = agg['hispanic_pop']/agg["population"]

    return agg

#     agg.to_csv(output_file)

#     return agg