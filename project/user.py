'''
User functions to search the locator
'''
import pandas as pd
import click

# initialize database
DB = pd.read_csv("processed_data/locator_database.csv")
# get list of Chicago neighborhoods
z = pd.read_csv("data/Neighborhood_Zri_AllHomesPlusMultifamily.csv", 
                usecols=["City", "RegionName"])

nbh = z[z.City == 'Chicago'].groupby('RegionName').groups.keys()
ptypes = ["4-Plex", "Apt", "Duplex", "House", "Townhouse", "TriPlex"]
dist_to_col = {0.25: "num_stops_quart_mi",
                0.5: "num_stops_half_mi",
                0.75: "num_stops_3quart_mi",
                1: "num_stops_1_mi"}

SAMPLE_OUTPUT = "output/sample_output_1.csv"

def search(criteria, output_filepath):
    '''
    Search for rental unit listings that satisfies specific
    criteria and stores results as a csv file.

    Inputs:
        criteria (Criteria): a Criteria object
        db (DataFrame): affordable rental unit locator database

    Returns:
        (DataFrame): listing search results

    '''
    rv = DB
    c = criteria.d

    if c["Address"]:
        f1 = rv.Address == c["Address"]
        rv = rv[f1]

    if c["Monthly Rent"]:
        lb_rent, ub_rent = c["Monthly Rent"]
        f2 = (rv["Monthly Rent"] >= lb_rent) & (rv["Monthly Rent"] <= ub_rent)
        rv = rv[f2]

    if c["Property Type"]:
        f3 = rv["Property Type"].isin(c["Property Type"])
        rv = rv[f3]

    if c["Bath"]:
        lb_bath, ub_bath = c["Bath"]
        f4 = (rv.Bath >= lb_bath) & (rv.Bath <= ub_bath)
        rv = rv[f4]

    if c["Bed"]:
        lb_bed, ub_bed = c["Bed"]
        f5 = (rv.Bed >= lb_bed) & (rv.Bed <= ub_bed)
        rv = rv[f5]

    if c["Available Now"]:
        f6 = rv.Availability == "Available Now"
        rv = rv[f6]

    if c["Neighborhood"]:
        f7 = rv.Neighborhood.isin(c["Neighborhood"])
        rv = rv[f7]

    if c["Has L-Stop within _ Mile"]:
        d = c["Has L-Stop within _ Mile"]
        f8 = rv[dist_to_col[d]] > 0
        rv = rv[f8]

    if rv.empty:
        print("No listing found")
        return
    else:
        cols_for_user = ['Address', 'Monthly Rent', 'Property Type',
                        'Bath', 'Bed', 'Availability', 'Contact',
                        'URL', 'State', 'County', 'City', 'Neighborhood',
                        '2016_evict_rate', 'er_percentile',
                        '2016_evict_filing_rate', 'efr_percentile',
                        '2011-2015_rent_perc_change', 
                        '2015-2019_rent_perc_change',
                        'potential_bad_landlord', 'bad_landlord_address',
                        'num_stops_quart_mi', 'num_stops_half_mi',
                        'num_stops_3quart_mi', 'num_stops_1_mi']
        rv = rv[cols_for_user]
        rv.to_csv(output_filepath)
        print("{} search results saved in {}".format(
                            len(rv), output_filepath))


class Criteria:
    '''
    Class for a user search criteria (query).

    '''
    def __init__(self):
        '''
        Constructor to initialize a Criteria object.
        
        Attribute: 
            d (Dict): search fields and values pairs, including:
            - Address: (str) full address. 
                    e.g. "1718 W 66th St 1, Chicago, IL 60636"
            - Monthly Rent: (tuple of ints)
                    rent within (<min of range>, <max of range>)
            - Property Type (list of str): 
                    a list containing one or more of the property types: 
                    "4-Plex", "Apt", "Duplex", "House", "Townhouse", "TriPlex"
            - Bath (tuple of floats): 
                    num of bathrooms within (<min of range>, <max of range>)
            - Bed (tuple of floats): 
                    num of bedrooms within (<min of range>, <max of range>)
            - Available Now (bool): 
                    whether the unit is available now
            - Neighborhood (list of str): 
                    a list containing one or more of the zillow neighborhoods
            - Has L-Stop within _ Mile (float): 
                    one of the following: 0.25, 0.5, 0.75, 1
  
        '''
        self.d = {"Address": None,
                "Monthly Rent": None,
                "Property Type": None,
                "Bath": None,
                "Bed": None,
                "Available Now": None,
                "Neighborhood": None,
                "Has L-Stop within _ Mile": None}

    def set_criteria(self, field_to_value):
        '''
        Set criteria with search field to value pairs.

        Input:
            field_to_value: (dict) search fields to value 
        '''
        for f, v in field_to_value.items():
            if f not in self.d:
                raise ValueError("{} is not a valid field\n".format(f) +
                    "Input one of the following fields: " +  
                    str(list(self.d)))
            else:
                if self.__has_correct_input(f, v):
                    self.d[f] = v


    def clear_criteria(self):
        '''
        Clear the search criteria dictionary.
        '''
        self.d = {"Address": None,
                "Monthly Rent": None,
                "Property Type": None,
                "Bath": None,
                "Bed": None,
                "Available Now": None,
                "Neighborhood": None,
                "Has L-Stop within _ Mile": None}
        print("All search fields are now cleared")


    def __has_correct_input(self, f, v):
        '''
        check input value types
        '''
        if f == "Address":
            if isinstance(v, str):
                return True
            else:
                raise TypeError("Wrong input type - " + \
                                "Address input should be a str")
        if f == "Monthly Rent":
            if not isinstance(v, tuple):
                raise TypeError("Wrong input type - " + \
                                "Monthly rent input should be a tuple")
            else:                
                lb_rent, ub_rent = v
                if isinstance(lb_rent, int) and isinstance(ub_rent, int):
                    if lb_rent <= ub_rent:
                        return True
                    else:
                        raise ValueError(
                            "Lower bound should be lower than upper bound")
                else:
                    raise TypeError("Wrong input type - " + \
                                    "rent min/max are not int")

        if f == "Property Type":
            if not isinstance(v, list):
                raise TypeError("Wrong intput type - " + \
                                "Property type input should be a list")
            elif all(elem in ptypes for elem in v):
                return True
            else:
                errmsg = "Wrong input value for Property Type\n" + \
                        "Input a list containing the following:\n" + \
                        str(ptypes)
                raise ValueError(errmsg)

        if f == "Bath":
            if not isinstance(v, tuple):
                raise TypeError("Wrong input type -" + \
                                " Bath input should be a tuple")
            else:
                lb_bath, ub_bath = v
                if isinstance(lb_bath, (int, float)) and \
                    isinstance(ub_bath, (int, float)):
                    if lb_bath <= ub_bath:
                        return True
                    else:
                        raise ValueError(
                            "Lower bound should be lower than upper bound")
                else:
                    raise TypeError("Wrong input type - " + \
                                    "bath min/max are not int/floats")
        
        if f == "Bed":
            if not isinstance(v, tuple):
                raise TypeError("Wrong input type - Bed input should be a tuple")
            else:
                lb_bed, ub_bed = v
                if isinstance(lb_bed, (int, float)) and \
                    isinstance(ub_bed, (int, float)):
                    if lb_bed <= ub_bed:
                        return True
                    else:
                        raise ValueError(
                            "Lower bound should be lower than upper bound")
                else:
                    raise TypeError("Wrong input type - " + \
                                    "bath min/max are not int/floats")

        if f == "Available Now":
            if isinstance(v, bool):
                return True
            else:
                raise TypeError("Wrong input type - " + \
                                "Available now input is not bool")

        if f == "Neighborhood":
            if not isinstance(v, list):
                raise TypeError("Wrong intput type - " + \
                                "Neighborhood input should be a list")
            elif all(elem in nbh for elem in v):
                return True
            else:
                errmsg = "Wrong input value for Neighborhood\n" + \
                        "Input a list containing the following: " + str(nbh)
                raise ValueError(errmsg)

        if f == "Has L-Stop within _ Mile":
            if v in dist_to_col:
                return True
            else:
                errmsg = "Wrong input value for distance to L-stop\n" + \
                        "Input one of the following values:\n" + \
                        str(list(dist_to_col))
                raise ValueError(errmsg)


    def __repr__(self):
        '''
        String representation of the search criteria.
        '''
        s = ""
        for k,v in self.d.items():
            s += "{}: {}\n".format(k, v)
        return  s


