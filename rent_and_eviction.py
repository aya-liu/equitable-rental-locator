'''
Processes rent and evictions data. 
Rent data is from Zillow. Evictions data is from Eviction Lab

Aya Liu, Bhargavi Ganesh, Vedika Ahuja
'''
import os
import pandas as pd

ZILLOW_FILE = "data/Neighborhood_Zri_AllHomesPlusMultifamily.csv"
EVICTIONS_FILE = "data/block-groups.csv"
ZILLOW_WITH_INC = "processed_data/zillow_rindex_with_increase.csv"

def read_and_process_rindex(csv_file, output_filename):
    '''
    Reads in the zillow rental indices at the neighborhood level

    Inputs:
        -zillow_file: csv file path
    Returns:
        -neighborhood_rent_df: pandas dataframe of rental data at neighborhood level
    '''
    col_types = {'RegionID': str,
                 'City': str,
                 'RegionName': str,
                 'State': str,
                 'Metro': str,
                 'CountyName': str,
                 '2011-01': 'Int64',
                 '2012-01': 'Int64', 
                 '2013-01': 'Int64',
                 '2014-01': 'Int64',
                 '2015-01': 'Int64',
                 '2016-01': 'Int64',
                 '2017-01': 'Int64',
                 '2018-01': 'Int64',
                 '2019-01': 'Int64'}

    cols_to_use = list(col_types)

    if os.path.exists(csv_file):
        neighborhood_rent_df = pd.read_csv(csv_file, usecols=cols_to_use, 
                                           dtype=col_types)
        neighborhood_rent_df['2011-2015'] = (
            neighborhood_rent_df['2015-01'] - neighborhood_rent_df['2011-01']
            ) / neighborhood_rent_df['2011-01']
        neighborhood_rent_df['2015-2019'] = (
            neighborhood_rent_df['2019-01'] - neighborhood_rent_df['2015-01']
            ) / neighborhood_rent_df['2015-01']

    neighborhood_rent_df.to_csv(output_filename)

    return neighborhood_rent_df


def read_and_process_evictions(csv_file):
    '''
    This function takes the block groups data (for Illinois) 
    downloaded from the evictions database and creates a 
    pandas dataframe. 

    Inputs:
        - evictions_file: csv file path
    Returns:
        - blockgroups_df: pandas dataframe of blockgroups
    '''
    col_types = {'GEOID': str,
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
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file, dtype=col_types)
        filtered_df = df[df['parent-location'].isin(['Cook County, Illinois'])]
        
    return filtered_df
