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

# def aggregate_block_group_data(block_group_df, geography_to_aggregate):
#     if geography_to_aggregate == "neighborhood":
#         merged = 
#     agg = grouped.agg({'population' : 'sum',
#                        'evictions' : 'sum',
#                        'eviction-filings' : 'sum',
#                        'renter-occupied-households' : 'sum',
#                        })

#      #calculate eviction rates
#     agg['eviction_rate'] = agg['evictions']/agg['renter-occupied-households']
#     agg['eviction_filing_rate'] = agg['eviction-filings']/agg['renter-occupied-households']

# def merge_block_group_locator(locator_database, block_group_filepath):
#     '''
#     Merge the block group data and the evictions data

#     Input:
#         - locator_database (pandas df)
#         - block_group_filepath (csv file)

#     Ouput:
#         - pandas df
#     '''
#     blocks = read_block_group_data("data/block-groups.csv")
#     merged = locator_database.merge(blocks, how="outer", on="GEOID", indicator=True)  

#     return merged



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

    agg = agg.filter(['num_cha_properties',
                      'Avg_monthly_rent',
                      'percent_wi_quart_mi_l_stop',
                      'percent_wi_half_mi_l_stop',
                      'num_props_w_potential_bad_landlord',
                      'eviction_rate', 
                      'eviction_filing_rate'], axis=1)

    agg = agg.sort_values(by='num_cha_properties', ascending=False)

    agg.to_csv(output_file)

    return agg

# def aggregate_table(locator_database, block_group_filepath, column_agg_by, output_file):
#     #generate transit dummy
#     blocks = read_block_group_data("data/block-groups.csv")

#     locator_database['stop_wi_quart_mi'] = np.where(locator_database['num_stops_quart_mi'] > 0, 1, 0)
#     locator_database['stop_wi_half_mi'] = np.where(locator_database['num_stops_half_mi'] > 0, 1, 0)
#     locator_database['stop_wi_half_mi'] = np.where(locator_database['num_stops_half_mi'] > 0, 1, 0)
#     # locator_database['bad_landlord']
#     #groupby and aggregate
#     merged = locator_database.merge(blocks, how="outer", on="GEOID", indicator=True)  

#     grouped = merged.groupby(column_agg_by)

#     agg_not_on_geoid = {'Address' : 'count', 
#                        'stop_wi_quart_mi' : 'sum', 
#                        'stop_wi_half_mi' : 'sum', 
#                        'population' : 'sum',
#                        'evictions' : 'sum',
#                        'eviction-filings' : 'sum',
#                        'renter-occupied-households' : 'sum',
#                     }

#     geoid_specific_vars = {'median-gross-rent': 'mean',
#                            'median-household-income': 'mean', 
#                            'median-property-value' : 'mean',
#                            'rent-burden': 'mean'}

#     combined_dict = {**agg_not_on_geoid, **geoid_specific_vars}

#     if column_agg_by == "GEOID":
#         agg = grouped.agg({**agg_not_on_geoid, **geoid_specific_vars
#                         })

#     else:
#        agg = grouped.agg({agg_not_on_geoid
#                 }) 

#     # 'population', 'eviction-filings', 'evictions'
#     #calculate rates
#     agg = agg.rename(columns={'Address': "num_cha_properties"})

#     agg['percent_wi_quart_mi_l_stop'] = agg['stop_wi_quart_mi']/agg['num_cha_properties']
#     agg['percent_wi_half_mi_l_stop'] = agg['stop_wi_half_mi']/agg['num_cha_properties']

#     # #calculate eviction rates
#     agg['eviction_rate'] = agg['evictions']/agg['renter-occupied-households']
#     agg['eviction_filing_rate'] = agg['eviction-filings']/agg['renter-occupied-households']
#     agg = agg.filter(['percent_wi_quart_mi_l_stop', 'percent_wi_half_mi_l_stop', 'num_cha_properties'], axis=1)
#     agg = agg.sort_values(by='num_cha_properties', ascending=False)

#     agg.to_csv(output_file)

#     return agg