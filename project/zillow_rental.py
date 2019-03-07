import requests
import bs4
import pandas


def create_request_dict():
	'''
	This function makes an API request for the Zillow GetRegionChildren API and 
	creaes a dictionary mapping regionIds to long lat
	'''
	#requesting API
	#make sure not to request too many times and lose priveleges
	#maybe need to add an if clause, to make sure that there is a good request that comes back?
	response = requests.get(
		"http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id=X1-ZWz182ehyks8az_2jayc&state=il&county=cook&childtype=neighborhood") 

	xml = response.content
	soup = bs4.BeautifulSoup(xml, features="xml")
	regions = soup.find_all("region")

	region_to_gps = {}

	for region in regions:
		region_id = region.find("id").text
		lon = region.find("longitude").text
		lat = region.find("latitude").text
		lon_float = float(lon)
		lat_float = float(lat)
		if region_id not in region_to_gps:
			region_to_gps[region_id] = (lon_float, lat_float)

	return region_to_gps


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



