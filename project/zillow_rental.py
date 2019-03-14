import requests
import bs4
import pandas




def read_and_process_rindex(csv_file):
    #csv_file = Neighborhood_Zri_AllHomesPlusMultifamily.csv
    regions_list = list(create_request_dict())
    #need to set col types
    neighborhood_rent_df = pd.read_csv(csv_file)
    filtered_df = zillow_rentals[zillow_rentals['RegionID'].isin(regions_list)]
    #add lon lat using the dictionary made above
    #remove key "139" (seems like a mistake)
    del region_to_gps['139'] 
    new_d = pd.series(region_to_gps) 

    return filtered_df



#next do calculations to find difference



