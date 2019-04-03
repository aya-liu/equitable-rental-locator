# Improving Chicago HCV Rental Locator for Fair and Equitable Housing



Chicago Housing Authority’s (CHA’s) Housing Choice Voucher (HCV) Program allows low-income families to rent quality housing in the private market with assistance from federal funds. Our project is meant to assist families in the HCV program find homes suitable to their needs by drawing together information from various publicly available data sources. A user can also use our program to generate aggregate reports and a map that provide information about the neighborhoods offering HCV housing.

Required Libraries
---

The program requires Python 3 to run. The following libraries need to be installed:
* pandas                    (0.23.4)
* numpy                     (1.15.4)
* geopandas                 (0.4.1)
* libgdal                   (2.4.0)
* matplotlib                (3.0.2)
* shapely                   (1.6.4)

To run the scraper, additional packages are required:
* urllib3               (1.24.1)
* requests              (2.21.0) 
* beautifulsoup4        (4.6.3)

Build the Database
---

Run the following command:
```sh
$ python3 build_database.py <output directory name>
```

Use the Database 
---

There are two separate tools for users to 1) search rental unit listings the database as a renter or to 2) get aggregated statistics as a researcher.
### For Renters: `user` Library
**Step 1: Creating and updating your search criteria**

* `user.Criteria()`: Initialize search criteria
* `Criteria.set_criteria(field_to_value_dictionary)`: Fill/update input to search fields. See below for parameter specifications.
* `Criteria.clear_criteria()`: Reset search criteria to none

The field to value dictionary for `set_criteria()` method includes search field to value pairs that restricts the results. Fields could be a subset of the field options listed below, and its corresponding value need to meet the following specifications:

| Key  | Value Type  | Value Description  |  Example
|---|---|---|---|
| Address  | str | Full address including city, state, zipcode | "Address" : "1718 W 66th St 1, Chicago, IL 60636"  |
|  Monthly Rent | tuple of int pair  | (< min >, < max >)  | "Monthly Rent": (900, 1100) |
| Property Type  | list of str  | List containing one or more of the property types: "4-Plex", "Apt", "Duplex", "House", "Townhouse", "TriPlex" | "Property Type": ["Duplex, Townhouse]" |
| Bath | tuple of int/float pairs | (< min >, < max >)| "Bath": (1.5, 2)
| Bed | tuple of int/float pairs |  (< min >, < max >) | "Bed": (1, 3)
|Available Now | bool | `True` for available now, `False` otherwise | "Available Now": True
|Neighborhood |list of str | List containing one or more of the zillow neighborhoods | "Neighborhood": [Woodlawn, Avalon Park]
|Has L-Stop within _ Mile |float| 0.25, 0.5, 0.75, or 1| "Has L-Stop within _ Mile": 0.5

**Step 2: Search the database using your criteria**  
Call `user.search(criteria, <output csv filename>)` to get a csv file of search listings that satisfy your criteria. 

Example:
```python
import user
c = user.Criteria() 
c.set_criteria({"Monthly Rent": (1000, 1200), 
   ...:         "Bath": (1, 3), 
   ...:         "Bed": (1, 2), 
   ...:         "Property Type": ["Apt", "House"], 
   ...:         "Neighborhood": ["Woodlawn", "Avalon Park"]})
user.search(c, output_path)
# add more criteria to further filter the results
c.set_criteria({"Available Now": True}) 
user.search(c, output_filepath) 
# reset criteria
user.clear_criteria() 
```

### For Researchers: Run `generate_report` 
Run the following command:
```python
python3 generate_report.py <output filename1> <output filename2> <output filename3> <map_filename>
```

This program aggregates information about the available HCV houses and the characteristics of the neighborhoods the houses reside in. Generate report exports three files:
* *output file 1 (csv)*: All neighborhoods in Chicago with neighborhood and HCV statistics if the neighborhoods had HCV houses available
* *output file 2 (csv)*: Compares neighborhoods with at least 10 HCV houses for rent to neighborhoods with less than 10 HCV homes for rent in Chicago
* *output file 3 (csv)*: Percent of HCV housing in neighborhoods with more and less than 20% poverty rate, with average neighborhood statistics
* *map filename (png)*: Map showing CHA properties by census block group


(Optional) Collect HCV Rental Listings
---
Run the following command to collect  listings from the [Chicago Housing Authority HCV Housing Finder](http://chicagoha.gosection8.com/Tenant/tn_Results.aspx):
```sh
$ python3 cha_scraper.py <output filename>
```

Data Dictionary
----
See [DATA_DICTIONARY.txt](https://mit.cs.uchicago.edu/capp30122-win-19/ayaliu-bganesh-vedikaa/blob/master/project/DATA_DICTIONARY.txt).

Authors
----

Aya Liu, Bhargavi Ganesh, and Vedika Ahuja

Data Sources
------------
[HCV Housing Finder, Chicago Housing Authority](http://chicagoha.gosection8.com/Tenant/tn_Results.aspx)
[Eviction Lab at Princeton University](https://data-downloads.evictionlab.org/)
[Zillow Rent Index (ZRI), Zillow Research](https://www.zillow.com/research/data/)
[List of 'L' Stops, CTA](https://data.cityofchicago.org/Transportation/CTA-System-Information-List-of-L-Stops/8pix-ypme)
[Problem Building Landlords, City of Chicago](https://www.chicago.gov/city/en/depts/bldgs/supp_info/building-code-scofflaw-list.html)

