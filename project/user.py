'''
User functions to search the locator

Aya Liu, Bhargavi Ganesh, and Vedika Ahuja

'''
import pandas as pd

# initialize database
DB = pd.read_csv("processed_data/locator_database.csv")

# setup constants for input type check
Z = pd.read_csv("data/Neighborhood_Zri_AllHomesPlusMultifamily.csv",
                usecols=["City", "RegionName"])
NBH = Z[Z.City == 'Chicago'].groupby('RegionName').groups.keys()
PTYPES = ["4-Plex", "Apt", "Duplex", "House", "Townhouse", "TriPlex"]
DIST_TO_COL = {0.25: "num_stops_quart_mi",
               0.5: "num_stops_half_mi",
               0.75: "num_stops_3quart_mi",
               1: "num_stops_1_mi"}

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
    cd = criteria.dict

    if cd["Address"]:
        fil1 = rv.Address == cd["Address"]
        rv = rv[fil1]

    if cd["Monthly Rent"]:
        lb_rent, ub_rent = cd["Monthly Rent"]
        fil2 = (rv["Monthly Rent"] >= lb_rent) & (
            rv["Monthly Rent"] <= ub_rent)
        rv = rv[fil2]

    if cd["Property Type"]:
        fil3 = rv["Property Type"].isin(cd["Property Type"])
        rv = rv[fil3]

    if cd["Bath"]:
        lb_bath, ub_bath = cd["Bath"]
        fil4 = (rv.Bath >= lb_bath) & (rv.Bath <= ub_bath)
        rv = rv[fil4]

    if cd["Bed"]:
        lb_bed, ub_bed = cd["Bed"]
        fil5 = (rv.Bed >= lb_bed) & (rv.Bed <= ub_bed)
        rv = rv[fil5]

    if cd["Available Now"]:
        fil6 = rv.Availability == "Available Now"
        rv = rv[fil6]

    if cd["Neighborhood"]:
        fil7 = rv.Neighborhood.isin(cd["Neighborhood"])
        rv = rv[fil7]

    if cd["Has L-Stop within _ Mile"]:
        dist = cd["Has L-Stop within _ Mile"]
        fil8 = rv[DIST_TO_COL[dist]] > 0
        rv = rv[fil8]

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
        self.dict = {"Address": None,
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
        for field, value in field_to_value.items():
            if field not in self.dict:
                raise ValueError("{} is not a valid field\n".format(field) +
                                 "Input one of the following fields: " +
                                 str(list(self.dict)))
            else:
                self.__has_correct_input(field, value)
                self.dict[field] = value

    def clear_criteria(self):
        '''
        Clear the search criteria dictionary.

        '''
        self.dict = {"Address": None,
                     "Monthly Rent": None,
                     "Property Type": None,
                     "Bath": None,
                     "Bed": None,
                     "Available Now": None,
                     "Neighborhood": None,
                     "Has L-Stop within _ Mile": None}
        print("All search fields are now cleared")

    def __has_correct_input(self, field, value):
        '''
        Check input value types for each search field and value and
        raise errors if any is invalid.

        Inputs:
            field: (str) search field
            value: (str) search value
        '''
        if field == "Address":
            if not isinstance(value, str):
                raise TypeError("Wrong input type - " + \
                                "Address input should be a str")

        if field == "Monthly Rent":
            if not isinstance(value, tuple):
                raise TypeError("Wrong input type - " + \
                                "Monthly rent input should be a tuple")
            lb_rent, ub_rent = value
            if not (isinstance(lb_rent, int) and isinstance(ub_rent, int)):
                raise TypeError("Wrong input type - " + \
                                "rent min/max are not int")
            elif lb_rent > ub_rent:
                raise ValueError(
                    "Lower bound should be lower than upper bound")

        if field == "Property Type":
            if not isinstance(value, list):
                raise TypeError("Wrong intput type - " + \
                                "Property type input should be a list")
            elif len(value) == 0:
                raise ValueError("Cannot set empty list as criteria")
            elif not all(elem in PTYPES for elem in value):
                errmsg = "Wrong input value for Property Type\n" + \
                        "Input a list containing the following:\n" + \
                        str(PTYPES)
                raise ValueError(errmsg)

        if field == "Bath":
            if not isinstance(value, tuple):
                raise TypeError("Wrong input type -" + \
                                " Bath input should be a tuple")
            lb_bath, ub_bath = value
            if not (isinstance(lb_bath, (int, float)) and \
                    isinstance(ub_bath, (int, float))):
                raise TypeError("Wrong input type - " + \
                                "bath min/max are not int/floats")
            elif lb_bath > ub_bath:
                raise ValueError(
                    "Lower bound should be lower than upper bound")

        if field == "Bed":
            if not isinstance(value, tuple):
                raise TypeError("Wrong input type - " + \
                                "Bed input should be a tuple")
            lb_bed, ub_bed = value
            if not (isinstance(lb_bed, (int, float)) and \
                    isinstance(ub_bed, (int, float))):
                raise TypeError("Wrong input type - " + \
                                "bath min/max are not int/floats")
            elif lb_bed > ub_bed:
                raise ValueError(
                    "Lower bound should be lower than upper bound")

        if field == "Available Now":
            if not isinstance(value, bool):
                raise TypeError("Wrong input type - " + \
                                "Available now input is not bool")

        if field == "Neighborhood":
            if not isinstance(value, list):
                raise TypeError("Wrong intput type - " + \
                                "Neighborhood input should be a list")
            elif len(value) == 0:
                raise ValueError("Cannot set empty list as criteria")
            elif not all(elem in NBH for elem in value):
                errmsg = "Wrong input value for Neighborhood\n" + \
                        "Input a list containing the following: " + str(NBH)
                raise ValueError(errmsg)

        if field == "Has L-Stop within _ Mile":
            if value not in DIST_TO_COL:
                errmsg = "Wrong input value for distance to L-stop\n" + \
                        "Input one of the following values:\n" + \
                        str(list(DIST_TO_COL))
                raise ValueError(errmsg)


    def __repr__(self):
        '''
        String representation of the search criteria.
        '''
        string = ""
        for field, value in self.dict.items():
            string += "{}: {}\n".format(field, value)
        return string
