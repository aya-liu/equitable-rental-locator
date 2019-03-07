import os
import pandas as pd


def cook_county_evictions():
	'''
	This function returns a dataframe with evictions for Cook County, IL.
	'''
	il_df = read_and_process_evictions("block-groups.csv")
	cook_county_df = block_groups_filter(il_df, ['Cook County, Illinois'])

	return cook_county_df 


def read_and_process_evictions(csv_file):
	'''
	This function takes the block groups data (for Illinois) 
	downloaded from the evictions database and creates a 
	pandas dataframe. 

	Inputs:
		- csv_file: csv file path
	Returns:
		- blockgroups_df: pandas dataframe of blockgroups
	'''
	col_types = {
	'GEOID': str,
	'year': str,
	'name': float,
	'parent-location': str,
	'population': float,
	'poverty-rate': float,
	'renter-occupied-households': float,
	'pct-renter-occupied': float,
	'median-gross-rent': float,
	'median-household-income': float,
	'median-property-value': float,
	'rent-burden': float,
	'pct-white': float,
	'pct-af-am': float,
	'pct-hispanic': float,
	'pct-am-ind': float,
	'pct-asian': float,
	'pct-nh-pi': float,
	'pct-multiple': float,
	'pct-other': float,
	'eviction-filings': float,
	'evictions': float,
	'eviction-rate': float,
	'eviction-filing-rate': float,
	'low-flag': int,
	'imputed': int,
	'subbed': int
	}

	if os.path.exists(csv_file):
		blockgroups_df = pd.read_csv(csv_file, dtypes=col_types)

	return blockgroups_df

	#fill NAs? NAs in gross rent and rent burdens

def block_groups_filter(df, counties):
	'''
	This function takes a pandas df of evictions in the state of IL
	and filters out counties of interest

	Inputs:
		-df: pandas dataframe of IL evictions
		-counties: list of counties to filter by
	'''
	filtered_df = df[df['parent-location'].isin(counties)]

	return filtered_df



