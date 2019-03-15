import pandas as pd
import numpy as np


#delete when done
def read_locator_db(filepath):
    ld = pd.read_csv(filepath, index_col="index")

    return ld

def aggregate_table(locator_database, column_agg_by):
    #generate transit dummy
    locator_database['stop_wi_quart_mi'] = np.where(locator_database['num_stops_quart_mi'] > 0, 1, 0)
    locator_database['stop_wi_half_mi'] = np.where(locator_database['num_stops_half_mi'] > 0, 1, 0)
    #groupby and aggregate
    grouped = locator_database.groupby(column_agg_by)

    agg = grouped.agg({'Address' : 'count', 
                       'stop_wi_quart_mi' : 'sum', 
                       'stop_wi_half_mi' : 'sum', 
                        })

    # agg = grouped.agg({'Address' : 'count', 
    #                    'stop_wi_quart_mi' : 'sum', 
    #                    'stop_wi_half_mi' : 'sum', 
    #                    'population' : 'sum',
    #                    'evictions' : 'sum'
    #                    'eviction-filings' : 'sum'
    #                     'renter-occupied-households' : 'sum'
    #                 })

    # 'population', 'eviction-filings', 'evictions'
    #calculate rates
    agg = agg.rename(columns={'Address': "num_cha_properties"})

    agg['percent_wi_quart_mi_l_stop'] = agg['stop_wi_quart_mi']/agg['num_cha_properties']
    agg['percent_wi_half_mi_l_stop'] = agg['stop_wi_half_mi']/agg['num_cha_properties']

    # #calculate eviction rates
    # agg['eviction_rate'] = agg['evictions']/agg['renter-occupied-households']
    # agg['eviction_filing_rate'] = agg['eviction-filings']/agg['renter-occupied-households']

    return agg


