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

def aggregate_table(locator_database, block_group_filepath, column_agg_by, output_file):
    #generate transit dummy
    blocks = read_block_group_data("data/block-groups.csv")

    merged = locator_database.merge(blocks, how="outer", on="GEOID", indicator=True)  

    locator_database['stop_wi_quart_mi'] = np.where(locator_database['num_stops_quart_mi'] > 0, 1, 0)
    locator_database['stop_wi_half_mi'] = np.where(locator_database['num_stops_half_mi'] > 0, 1, 0)
    locator_database['stop_wi_half_mi'] = np.where(locator_database['num_stops_half_mi'] > 0, 1, 0)
    # locator_database['bad_landlord']
    #groupby and aggregate
    grouped = locator_database.groupby(column_agg_by)

    agg_not_on_geoid = {'Address' : 'count', 
                       'stop_wi_quart_mi' : 'sum', 
                       'stop_wi_half_mi' : 'sum', 
                       'population' : 'sum',
                       'evictions' : 'sum',
                       'eviction-filings' : 'sum',
                       'renter-occupied-households' : 'sum',
                    }

    geoid_specific_vars = {'median-gross-rent': 'mean',
                           'median-household-income': 'mean', 
                           'median-property-value' : 'mean',
                           'rent-burden': 'mean'}

    combined_dict = {**agg_not_on_geoid, **geoid_specific_vars}

    if column_agg_by == "GEOID":
        agg = grouped.agg({**agg_not_on_geoid, **geoid_specific_vars
                        })

    else:
       agg = grouped.agg({agg_not_on_geoid
                }) 

    # 'population', 'eviction-filings', 'evictions'
    #calculate rates
    agg = agg.rename(columns={'Address': "num_cha_properties"})

    agg['percent_wi_quart_mi_l_stop'] = agg['stop_wi_quart_mi']/agg['num_cha_properties']
    agg['percent_wi_half_mi_l_stop'] = agg['stop_wi_half_mi']/agg['num_cha_properties']

    # #calculate eviction rates
    agg['eviction_rate'] = agg['evictions']/agg['renter-occupied-households']
    agg['eviction_filing_rate'] = agg['eviction-filings']/agg['renter-occupied-households']
    agg = agg.filter(['percent_wi_quart_mi_l_stop', 'percent_wi_half_mi_l_stop', 'num_cha_properties'], axis=1)
    agg = agg.sort_values(by='num_cha_properties', ascending=False)

    agg.to_csv(output_file)

    return agg


