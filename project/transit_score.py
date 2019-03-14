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


def transit_score(orig_lat, orig_lon, apartments_dataset, L_stations, bus_stations):
    '''
    calculates manhattan distance between the location and 
    the l stations and bustations and calculates a transit score based 
    on the number of nearby stations

    Score is based on the percentile ranking of close transit stations
    in the city of Chicago

    ex (99) means the location is closer to more stops than 99% of other
    places in the dataset. (reconsider how to do this - might want to create a raw score)
    '''
    pass

def clean_L_stations(L_stations_csv):
    '''
    Takes in L_stations_csv, cleans it so it ready to merge 
    (has separate lat and long columns)

    Save as a pandas file
    '''
    L_stations = pd.read_csv(L_stations_csv)
    L_stations["Location"] = L_stations["Location"].str.replace("\(", "")
    L_stations["Location"] = L_stations["Location"].str.replace("\)", "")
    new = L_stations["Location"].str.split(",", expand=True)
    L_stations["Lat"] = new[0]
    L_stations["Long"] = new[1]
    L_stations.drop(columns = ["Location"], inplace=True)


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

def merge_apartments_data(apartments_dataset, other_set):
    '''
    Merge apartments with L stations and/or bustations.
    m:m merge (every obs in apartments_dataset has to merge with 
    every obs in the L_stations data)

    Calculate manhatttan dist and drop everything less than .5 miles away from
    each location
    '''
    pass


def cartesian_product(*arrays):
    '''
    Takes 2 numpy arrays and returns an array that is a combination of every
    observation in the first with every observation in the 2nd.

    Code from: 
    https://stackoverflow.com/questions/11144513/numpy-cartesian-product-of-x-and-y-array-points-into-single-array-of-2d-points/11146645#11146645
    and
    https://stackoverflow.com/questions/52104737/python-how-to-expand-a-pandas-dataframes-rows-to-include-all-combinations-of/52104849
    '''
    la = len(arrays)
    dtype = np.result_type(*arrays)
    arr = np.empty([len(a) for a in arrays] + [la], dtype=dtype)
    for i, a in enumerate(np.ix_(*arrays)):
        arr[...,i] = a

    v = arr.reshape(-1, la)

    df = pd.DataFrame(data=v[:], columns=["apt_ind", "STOP_ID"])
    df.STOP_ID = df.STOP_ID.astype(int)

    return df


def create_cross_join(apartments, l_stations):
    cross_product_ind = cartesian_product(apartments.index, l_stations["STOP_ID"])

    l_stations_fil = l_stations.filter(["STOP_ID", "Lat", "Long"], axis=1)
    
    apartments["apt_ind"] = apartments.index
    apartments_fil = apartments.filter(["apt_ind", "Lat", "Long"], axis=1)
    merged = cross_product_ind.join(apartments_fil, on="apt_ind", lsuffix= "apt_")
    merged = merged.rename(index=float, columns={"Lat": "Lat_apt", "Long": "Long_apt"})

    merged = merged.merge(l_stations_fil, how="outer", on="STOP_ID")
    merged = merged.rename(index=float, columns={"Lat": "Lat_stop", "Long": "Long_stop"})
    merged.Lat_stop = merged.Lat_stop.astype(float)
    merged.Long_stop = merged.Long_stop.astype(float)

    return merged
    # #create numpy arrays from the l_stations and apartments
    # l_stations_arr = l_stations_ind.reset_index().values
    # apt_arr = apartments_.reset_index().values

    # v = cartesian_product(l_stations_arr, apt_a
    # idx = pd.MultiIndex.from_arrays([v[:, 0], v[:, 1]])
    # df.set_index(['key1', 'key2']).reindex(idx)

    # l_stations_sub = l_stations.filter(["STOP_ID", "Lat", "Long"], axis=1)


    # return df
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
    #only keep bus stops that are within .5 miles of apartments
    # sub[]
    # quart = np.where(sub['distance'] <= .25, 1, 0)
    # sub['wi_quart_mi'] = 0
    # sub['wi_half_mi'] = 0

    # sub.loc[sub['distance'] <= .5, 'wi_half_mi'] = 1
    # sub.loc[sub['distance'] <= .25, 'wi_quart_mi'] = 1

    # sub
    # sub.loc

    # df.loc[df['used'] == 1.0, 'alert'] = 'Full'
    df['wi_half_1_mi'] = np.where(df['distance'] <= 1, 1, 0)    
    df['wi_half_3_quart'] = np.where(df['distance'] <= .75, 1, 0)
    df['wi_half_mi'] = np.where(df['distance'] <= .5, 1, 0)
    df['wi_quart_mi'] = np.where(df['distance'] <= .25, 1, 0)

    sub_agg = df.groupby('apt_ind', as_index=False)[['wi_quart_mi', 'wi_half_mi', 'wi_half_3_quart', 'wi_half_1_mi']].sum()

    # df['profitable'] = np.where(df['price']>=15.00, True, False)
    return sub_agg


def go(apartments, l_stations):
    '''
    put it all together
    '''
    df = create_cross_join(apartments, l_stations)
    df['distance'] = haversine(
                    df["Long_apt"], df["Lat_apt"], df["Long_stop"], df["Lat_stop"])

    grouped = assign_transit_score(df)

    return grouped