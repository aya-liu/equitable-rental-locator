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

def search(criteria, db, output_filepath):
    '''
    Search for rental unit listings that satisfies specific
    criteria and stores results as a csv file.

    Inputs:
        criteria (Criteria): a Criteria object
        db (DataFrame): affordable rental unit locator database

    Returns:
        (DataFrame): listing search results

    '''
    rv = db
    c = criteria.d

    if c["Address"]:
        if isinstance(c["Address"], str):
            f1 = rv.Address == c["Address"]
            rv = rv[f1]
        else:
            raise TypeError("Wrong input type - Address input is not a str")

    if c["Monthly Rent"]:
        if isinstance(c["Monthly Rent"], tuple):
            lb_rent, ub_rent = c["Monthly Rent"]
            if isinstance(lb_rent, int) and isinstance(ub_rent, int):
                f2 = (rv["Monthly Rent"] >= lb_rent) & \
                        (rv["Monthly Rent"] <= ub_rent)
                rv = rv[f2]
            else:
                raise TypeError("Wrong input type - rent min/max are not int")
        else:
            raise TypeError("Wrong input type - \
                                    Monthly rent input is not a tuple")

    if c["Property Type"]:
        if all(elem in ptypes for elem in c["Property Type"]):
            f3 = rv["Property Type"].isin(c["Property Type"])
            rv = rv[f3]
        else:
            errmsg = "Wrong input value for Property Type\n" + \
                    "Input a list containing the following:\n" + \
                    str(ptypes)
            raise ValueError(errmsg)

    if c["Bath"]:
        if isinstance(c["Bath"], tuple):
            lb_bath, ub_bath = c["Bath"]
            if isinstance(lb_bath, (int, float)) and \
                isinstance(ub_bath, (int, float)):
                f4 = (rv.Bath >= lb_bath) & (rv.Bath <= ub_bath)
                rv = rv[f4]
            else:
                raise TypeError("Wrong input type - \
                                        bath min/max are not int/floats")
        else:
            raise TypeError("Wrong input type - Bath input is not a tuple")

    if c["Bed"]:
        if isinstance(c["Bed"], tuple):
            lb_bed, ub_bed = c["Bed"]
            if isinstance(lb_bed, (int, float)) and \
                isinstance(ub_bed, (int, float)):
                f5 = (rv.Bed >= lb_bed) & (rv.Bed <= ub_bed)
                rv = rv[f5]
            else:
                raise TypeError("Wrong input type - \
                                        bed min/max are not int/floats")
        else:
            raise TypeError("Wrong input type - Bed input is not a tuple")

    if c["Available Now"]:
        if isinstance(c["Available Now"], bool):
            f6 = rv.Availability == "Available Now"
            rv = rv[f6]
        else:
            raise TypeError("Wrong input type - \
                                        Available now input is not bool")

    if c["Neighborhood"]:
        if all(elem in nbh for elem in c["Neighborhood"]):
            f7 = rv.Neighborhood.isin(c["Neighborhood"])
            rv = rv[f7]
        else:
            errmsg = "Wrong input value for Neighborhood\n" + \
                    "Input a list containing the following: " + str(nbh)
            raise ValueError(errmsg)

    if c["Has L-Stop within _ Mile"]:
        d = c["Has L-Stop within _ Mile"]
        if d in dist_to_col:
            f8 = rv[dist_to_col[d]] > 0
            rv = rv[f8]
        else:
            errmsg = "Wrong input value for distance to L-stop\n" + \
                    "Input one of the following values:\n" + \
                    str(list(dist_to_col))
            raise ValueError(errmsg)

    if rv.empty:
        print("No listing found.")
        return
    else:
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

    def update_criteria(self, field_to_value):
        '''
        Update criteria with search field to value pairs.

        Input:
            field_to_value: (dict) search fields to value 
        '''
        for f, v in field_to_value.items():
            if f in self.d:
                self.d[f] = v
            else:
                raise ValueError("{} is not a valid field\n".format(f) +
                    "Should be one of the following fields: " +  
                    str(list(self.d)))

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

    def __repr__(self):
        '''
        String representation of the search criteria.
        '''
        s = ""
        for k,v in self.d.items():
            s += "{}: {}\n".format(k, v)
        return  s


