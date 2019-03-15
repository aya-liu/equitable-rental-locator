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
    
    # agg = grouped.agg({'Addresss' : 'count', 
    #                    'stop_wi_quart_mi' : 'sum', 
    #                    'stop_wi_half_mi' : 'sum', 
    #                     })
    # 'population', 'eviction-filings', 'evictions'
    return agg