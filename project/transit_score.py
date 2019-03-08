'''

This program takes a gps coordinate and applies a score to 
that location based on how many bus stops and L stations are 
within a certain vicinity of the locaton.
'''

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
	pass

def clean bus_stations(bus_stations_csv):
	'''
	Takes bus stations csv, cleans it so it's ready to merge
	(has separate lat and long columns).

	Save as a pandas file
	'''

def merge_apartments_data(apartments_dataset, other_set):
	'''
	Merge apartments with L stations and/or bustations.
	m:m merge (every obs in apartments_dataset has to merge with 
	every obs in the L_stations data)

	Calculate manhatttan dist and drop everything less than .5 miles away from
	each location
	'''
	
