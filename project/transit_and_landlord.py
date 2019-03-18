'''
Adds dummy variables about L stop locations and problem landlords to the
locator database.

Data from:
    - L stations list
    - Problem Landlord list
    - Full locator database of HCV apartments
'''
import pandas as pd
import numpy as np


def clean_L_stations(L_stations_csv):
    '''
    Takes in L_stations_csv, cleans it so it ready to merge 
    (has separate lat and long columns)
    
    Inputs: L_stations_csv (csv)
    Returns: L_stations (DataFrame)
    '''
    L_stations = pd.read_csv(L_stations_csv, index_col="STOP_ID")
    L_stations["Location"] = L_stations["Location"].str.replace("\(", "")
    L_stations["Location"] = L_stations["Location"].str.replace("\)", "")
    new = L_stations["Location"].str.split(",", expand=True)
    L_stations["Lat"] = new[0]
    L_stations["Long"] = new[1]
    L_stations.drop(columns=["Location"], inplace=True)
    L_stations["Lat"] = L_stations["Lat"].astype(float)
    L_stations["Long"] = L_stations["Long"].astype(float)

    return L_stations


def cartesian_product(df1_ind, df2_ind):
    '''
    Takes 2 numpy arrays and returns an array that is a combination of every
    observation in the first with every observation in the 2nd.

    Source:: 
    https://stackoverflow.com/questions/11144513/numpy-cartesian-product-of-x
    -and-y-array-points-into-single-array-of-2d-points/11146645#11146645
    https://stackoverflow.com/questions/52104737/python-how-to-expand-a-pandas
    -dataframes-rows-to-include-all-combinations-of/52104849

    Inputs: 
        - df1_ind, df2_ind (DataFrame)
    Returns: df (DataFrame)
    '''
    dtype = np.result_type(df1_ind, df2_ind)
    arr = np.empty([len(a) for a in (df1_ind, df2_ind)] + [2], dtype=dtype)
    for i, a in enumerate(np.ix_(df1_ind, df2_ind)):
        arr[..., i] = a
    v = arr.reshape(-1, 2)    
    df = pd.DataFrame(data=v[:], columns=["ind1", "ind2"], dtype=str)

    return df


def create_cross_join(df1, df2, suffixes, additional_columns=None):
    '''
    creates a dataframe with the columns: 
    df1 index, df2 index, lat1, long1, lat2, long2
    If additional columns are indicated in the parameters, then
    the final dataset includes those columns from both the 
    dataframe 1 and 2.

    Inputs: 
        df1: (DataFrame)
        df2: (DataFrame)
        suffixes: (tuple) suffixes associated with df1 and df1 for 
                          final dataframe
        additional_columns: (list) columns to include in final dataframe
                                   in addition df1, df2.

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

    suffix1, suffix2 = suffixes
    re_ind1 = "ind" + suffix1
    re_ind2 = "ind" + suffix2
    merged = merged.rename(columns={"ind1": re_ind1, "ind2": re_ind2})

    return merged


def haversine(lon1, lat1, lon2, lat2):
    '''
    Calculates the haversine distance between 2 lat, lon points.
    Source: CS122 pa5

    Inputs: lon1, lat1, lon2, lat2 (int)
    Returns: mi (int)
    '''
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
    Takes dataframe and add dummy variables for apartments with stations 
    within 1/4, 1/2, 3/4 and 1 mile aways. Intermediate dataframe.

    Inputs: 
        - df (DataFrame)
    Output: 
        - sub_agg (DataFrame)
    '''
    df['wi_1_mi'] = np.where(df['distance'] <= 1, 1, 0)    
    df['wi_3_quart'] = np.where(df['distance'] <= .75, 1, 0)
    df['wi_half_mi'] = np.where(df['distance'] <= .5, 1, 0)
    df['wi_quart_mi'] = np.where(df['distance'] <= .25, 1, 0)

    sub_agg = df.groupby('ind_apt', as_index=False)[['wi_quart_mi', 
                                                     'wi_half_mi', 
                                                     'wi_3_quart', 
                                                     'wi_1_mi']].sum()

    return sub_agg


def read_clean_landlords(problem_landlords_filepath):
    '''
    Read and clean the problem lands lords csv from the CHA site.

    Data Source: 
    https://www.chicago.gov/city/en/depts/bldgs/supp_info/building-code
    -scofflaw-list.html
    
    Input: problem_landlords_filepath (csv)
    Returns: landlords_sub (DataFrame)
    '''
    landlords = pd.read_csv(problem_landlords_filepath)
    landlords = landlords.rename(columns={"LONGITUDE": "Long", 
                                          "LATITUDE": "Lat", 
                                          "ADDRESS": "Address"})
    landlords_sub = landlords.filter(["Address", "Long", "Lat"], axis=1)

    return landlords_sub


def landlords_apt_fuzzy_match(apt_df, landlords_df, threshold):
    '''
    Do a fuzzy match by gps coordinates on the dataset with problem
    landlords and apartments. Match landlords and apartments that are
    within .01 miles of each other. 

    Inputs: 
        - apt_df: (DataFrame)
        - landlords_df: (DataFrame)
        - threshold: (int) 
    Returns: df_fil (DataFrame)
    '''
    df = create_cross_join(apt_df, landlords_df, ("_apt", "_ll"), 
                           additional_columns=["Address"])
    df['distance'] = haversine(df["Long_apt"], df["Lat_apt"], df["Long_ll"], 
                               df["Lat_ll"])
    df['v_close'] = np.where(df['distance'] <= threshold, 1, 0)
    df_fil = df[df['v_close'] == 1]
    df_fil = df_fil.filter(['ind_apt', 'Address_ll'], axis=1)
    df_fil['potential_bad_landlord'] = True
    df_fil = df_fil[['ind_apt', 'potential_bad_landlord', 'Address_ll']]

    return df_fil


def compute_num_stations(cha, l_stations_filepath):
    '''
    Compute the number of stations within .25, .5, .75 and 1 mile of each
    housing unit.
    
    Inputs: 
        - cha: (DataFrame)
        - l_stations_filepath (csv)

    Returns: apt_w_l_stations (DataFrame)
            
    '''
    l_stations = clean_L_stations(l_stations_filepath)
    df = create_cross_join(cha, l_stations, ("_apt", "_stop")) 
    df['distance'] = haversine(df["Long_apt"], df["Lat_apt"], 
                               df["Long_stop"], df["Lat_stop"])
    apt_w_l_stations = assign_transit_score(df)

    return apt_w_l_stations


def flag_potential_bad_landlord(cha, problem_landlords_filepath, threshold):
    '''
    Flag units with potential problem landlords. Units are flagged if
    they are within .015 miles of the bad landlord locations on record.
    Returns a  DataFrame with housing unit indices mapped to whether the unit
    is flagged for potential problem landlords, and if so, the problem
    address on record for manual comparison with the unit address.

    Inputs: 
        - cha: (DataFrame) 
        - problem_landlords_filepath (csv)
        - threshold (float): an acceptable error threshold for the fuzzy 
                            match between units and problem landlords

    Returns: 
        - apt_to_ll: (DataFrame)
    '''
    landlords_df = read_clean_landlords(problem_landlords_filepath)
    ll_match = landlords_apt_fuzzy_match(cha, landlords_df, threshold)
    apts = cha.reset_index().rename(columns={"index": "ind_apt"}).filter(["ind_apt"])
    apt_to_ll = pd.merge(apts, ll_match, how="left", on="ind_apt")
    apt_to_ll['potential_bad_landlord'].fillna(False, inplace=True)

    return apt_to_ll
