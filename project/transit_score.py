import pandas as pd
import re
import pickle
import numpy as np
from math import radians, cos, sin, asin, sqrt, ceil
import vincenty
# import process_and_merge
'''

This program takes a gps coordinate and applies a score to 
that location based on how many bus stops and L stations are 
within a certain vicinity of the locaton.
'''
def load_and_clean_cha(CHA_filename):
    '''
    Load and clean CHA data.
    Input:
        (str) file name of the raw CHA housing unit data
    Returns:
        (DataFrame) clean CHA data
    '''
    # load raw CHA data
    with open(CHA_filename, "rb") as f:       
        d = pickle.load(f)
    cha = pd.DataFrame.from_dict(data = d, orient = "index")
    
    # clean CHA data
    cols = ['Address','Monthly Rent','Property Type','Bath','Bed',
        'Voucher Necessary','Availability','Contact','URL','Lat','Long']
    cha = cha[cols]
    cha.Long = -1 * cha.Long

    # correct an one-off location error
    cha.loc["4545145", "Long"] = -87.66593 
    cha.loc["4545145", "Lat"] = 41.772175
    
    return cha


def clean_L_stations(L_stations_csv):
    '''
    Takes in L_stations_csv, cleans it so it ready to merge 
    (has separate lat and long columns)

    Save as a pandas file
    '''
    L_stations = pd.read_csv(L_stations_csv, index_col = "STOP_ID")
    L_stations["Location"] = L_stations["Location"].str.replace("\(", "")
    L_stations["Location"] = L_stations["Location"].str.replace("\)", "")
    new = L_stations["Location"].str.split(",", expand=True)
    L_stations["Lat"] = new[0]
    L_stations["Long"] = new[1]
    L_stations.drop(columns = ["Location"], inplace=True)
    L_stations["Lat"] = L_stations["Lat"].astype(float)
    L_stations["Long"] = L_stations["Long"].astype(float)

    return L_stations


def clean_bus_stations(bus_stations_csv):
    '''
    Takes bus stations csv, cleans it so it's ready to merge
    (has separate lat and long columns).

    Save as a pandas file
    '''
    df = pd.read_csv(bus_stations_csv)
    df = df.rename(index=float, columns={"POINT_Y": "Lat", "POINT_X": "Long"})

    return df


def cartesian_product(df1_ind, df2_ind):
    '''
    Takes 2 numpy arrays and returns an array that is a combination of every
    observation in the first with every observation in the 2nd.

    Code from: 
    https://stackoverflow.com/questions/11144513/numpy-cartesian-product-of-x-and-y-array-points-into-single-array-of-2d-points/11146645#11146645
    and
    https://stackoverflow.com/questions/52104737/python-how-to-expand-a-pandas-dataframes-rows-to-include-all-combinations-of/52104849
    '''
    dtype = np.result_type(df1_ind, df2_ind)
    arr = np.empty([len(a) for a in (df1_ind, df2_ind)] + [2], dtype=dtype)
    for i, a in enumerate(np.ix_(df1_ind, df2_ind)):
        arr[...,i] = a

    v = arr.reshape(-1, 2)

    # df = pd.DataFrame(data=v[:], columns=["apt_ind", "STOP_ID"])
    
    df = pd.DataFrame(data=v[:], columns=["ind1", "ind2"], dtype=str)

    # df.STOP_ID = df.STOP_ID.astype(int)

    return df


def create_cross_join(df1, df2, suffixes, additional_columns=None):
    '''
    creates a dataframe with the columns: 
        df1 index, df2 index, lat1, long1, lat2, long2
        If additional columns are indicated in the paratments, then
        the final dataset includes those columns from both the 
        dataframe 1 and 2.

    Inputs: 
        df1: pandas dataframe
        df2: pandas dataframe
        suffixes: a tuple of the suffixes on the columns in the final dataframe
                associated with the df1 and df2 respectively.
        additional_columns: list of columns to include in final dataframe
            in addition to the ones listed.

    '''
    cross_product_ind = cartesian_product(df1.index, df2.index)

    filt_list = ["Lat", "Long"]
    if additional_columns:
        filt_list += additional_columns

    df1_fil = df1.filter(filt_list, axis=1)
    df1_fil['ind1'] = df1.index.astype(str)
    df2_fil = df2.filter(filt_list, axis=1)
    df2_fil['ind2'] = df2.index.astype(str)
    
    merged = cross_product_ind.merge(df1_fil, how="outer", on="ind1")
    merged = merged.merge(df2_fil, how="outer", on="ind2", suffixes=suffixes)

    #rename index columns to reflect suffixes
    suffix1, suffix2 = suffixes
    re_ind1 = "ind" + suffix1
    re_ind2 = "ind" + suffix2
    merged = merged.rename(columns={"ind1": re_ind1, "ind2": re_ind2})
    #make all the lat longs floats
    # merged = merged.astype({"Lat_df1": float, "Lat_df2":float, "Long_df1":float, "Long_df2":float})

    return merged


def haversine(lon1, lat1, lon2, lat2):
    '''
    Because there are so many row, I am avoiding the apply function
    that takes significantly more time than directly doing these calculations
    '''
    #convert lat and long to radians
    # gps = [df_lon1, df_lat1, df_lon2, df_lat2]
    # for row in gps:
        
    #     print(row.dtype)
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/ 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/ 2)**2
    c = 2 * np.arcsin(np.sqrt(a))

    # 6367 km is the radius of the Earth
    km = 6367 * c
    mi = 0.621371 *km

    return mi

def assign_transit_score(df):
    '''
    Takes dataframe and assigns transit scores

    Amenities within a 5 minute walk (.25 miles) are given
    maximum points. A decay function is used to give points
    to more distant amenities, with no points given after
    a 30 minute walk.
    '''
    df['wi_1_mi'] = np.where(df['distance'] <= 1, 1, 0)    
    df['wi_3_quart'] = np.where(df['distance'] <= .75, 1, 0)
    df['wi_half_mi'] = np.where(df['distance'] <= .5, 1, 0)
    df['wi_quart_mi'] = np.where(df['distance'] <= .25, 1, 0)

    sub_agg = df.groupby('ind_apt', as_index=False)[['wi_quart_mi', 'wi_half_mi', 'wi_3_quart', 'wi_1_mi']].sum()

    return sub_agg

def read_clean_landlords(problem_landlords_filepath):
    '''
    Read and clean the problem lands lords csv from the 
    CHA site.

    Downloaded from: 
    https://www.chicago.gov/city/en/depts/bldgs/supp_info/building-code-scofflaw-list.html
    '''
    landlords = pd.read_csv(problem_landlords_filepath)
    landlords = landlords.rename(columns={"LONGITUDE": "Long", "LATITUDE": "Lat", "ADDRESS": "Address"})
    landlords_sub = landlords.filter(["Address", "Long", "Lat"], axis=1)

    return landlords_sub


def landlords_apt_fuzzy_match(apt_df, landlords_df, output_filepath):
    '''
    Do a fuzzy match by gps coordinates on the dataset with problem
    landlords and apartments. Match landlords and apartments that are
    within .1 miles of each other. Export a csv with these matches,
    and then manually check if the addresses are the same.
    '''
    df = create_cross_join(apt_df, landlords_df, ("_apt", "_ll"), \
                           additional_columns=["Address"])
    df['distance'] = haversine(df["Long_apt"], df["Lat_apt"], df["Long_ll"], \
                             df["Lat_ll"])
    df['v_close'] = np.where(df['distance'] <= .05, 1, 0)
    df_fil = df[df['v_close']==1]

    df_fil = df_fil.filter(["ind_apt", "ind_ll", "Address_apt", "Address_ll", \
                            "distance"], axis=1).reset_index(drop=True)

    #output a csv to manually check if these apartments are the same
    df_fil.to_csv(output_filepath)

    return df_fil


def import_and_join_manual_landlord_check(apt_df, manual_check_filepath_csv):
    '''
    Imports the csv with apartment addresses and problem landlords
    within .1 miles of each other (the csv outputted by landlords_apt_fuzzy_match
    , after a person has manually checked whether the apartment
    and landlord addresses are the same.

    Input:
        -apt: full apartment dataframe
        -manual_check_filepath_csv: filepath of the csv file with a manually
            checked column - 1 for same_address, NaN if not 
    '''
    df = pd.read_csv(manual_check_filepath_csv)
    df['bad_landlord'] = np.where(df['same_address'] == 1, True, False)
    sub = df.filter(["ind_apt", "bad_landlord"], axis=1)
    sub["ind_apt"] = sub["ind_apt"].astype(str)
    # return sub
    merged = apt_df.merge(sub, how="outer", on="ind_apt") 

    merged['bad_landlord'] = merged['bad_landlord'].fillna(False)

    return merged


def go(apartments, 
       l_stations_filepath, 
       output_filepath,
       problem_landlords_filepath=None, 
       landlords_apt_fuzzy_match_csv_output=None, 
       manual_checked_bad_landlord_filepath=None
       ):
    '''
    default filenames to use:
    go(apt,
       "data/CTA_L_stops_locations.csv",
       "processed_data/apt_l_stops_landlord.csv", 
       "data/problem_landlords.csv", 
       "data/manual_check_bad_landlords.csv", 
       "data/manual_check_bad_landlords_done.csv")

    Inputs: 
        - apartments (pandas dataframe that has been create and cleaned in
                        process_and_merge.py 
        - l_stations_filepath: filepath for csv of the locations of the l_stations
        - bad_landlord_filepath: This is the csv containing a list of the 
            problem landlords identified by the city, with the addresses of the 
            landlord's buildings, and the lat and long coordinates.
            Note: In order to match these landlord addresses to apartment
                addresses, we output a csv with a list of the apartments
                matched to problem landlord buildings that are within .01 miles
                from each other. Then we do a manual check to see if in 
                fact the apartment has the same address and is in the same 
                building as the problem landlord. If a programmer does not want
                to do this manual check, then the bad_landlord column will not
                be included in the final output.
        - manual_checked_bad_landlord_filepath: This is the csv with that
            contains the apartments within .05 miles of a bad landlord, AFTER
            a programmer has manually checked whether the apartments addresses
            are the same as a bad landlord address. 
            Note: We have done this, but a programmer does not have to do this
            step. If they do not, then the bad_landlord column will not be 
            included in the final ouput.

        Outputs:
            
    '''
    l_stations = clean_L_stations(l_stations_filepath)
    df = create_cross_join(apartments, l_stations, ("_apt", "_stop")) 
    df['distance'] = haversine(
                    df["Long_apt"], df["Lat_apt"], 
                    df["Long_stop"], df["Lat_stop"])
    apt_w_l_stations = assign_transit_score(df)

    if problem_landlords_filepath:
        #exports a csv with a fuzzy match of apartments with bad landlord
        #buildings
        landlords_df = read_clean_landlords(problem_landlords_filepath)
        landlords_apt_fuzzy_match(apartments,
                                  landlords_df, 
                                  landlords_apt_fuzzy_match_csv_output)
        #uses the csv above that has been manually checked and saved as a new file 
        apt_w_l_stations = import_and_join_manual_landlord_check(
                                            apt_w_l_stations, 
                                            manual_checked_bad_landlord_filepath)

    apt_w_l_stations.to_csv(output_filepath)

    return apt_w_l_stations