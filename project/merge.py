'''
Merge all data sources
'''

import pandas as pd
from process_cha_data import process_cha_data
import clean_rent_eviction as clean
import transit_score as ts
pd.options.display.max_rows = 999

# data sources filenames
CHA_DICT_RAW = "data/CHA_rental_data.obj"
BLOCKS_GEOFILE = "data/block-groups.geojson"
ZILLOW_GEOFILE = "data/ZillowNeighborhoods-IL.shp"
ZILLOW_FILE = "data/Neighborhood_Zri_AllHomesPlusMultifamily.csv"
EVICTIONS_FILE = "data/block-groups.csv"
# processed data filenames
CHA_CLEAN = "processed_data/CHA_clean.csv"
CHA_WITH_KEYS = "processed_data/CHA_with_merge_keys.csv"
ZILLOW_WITH_INC = "processed_data/zillow_rindex_with_increase.csv"
CHA_WITH_EV_RINDEX = "processed_data/CHA_with_evction_rindex.csv"


def main():
	'''
	Merge all data sources
	'''
	cha = process_cha_data(CHA_DICT_RAW, BLOCKS_GEOFILE, ZILLOW_GEOFILE,
                                   CHA_CLEAN, CHA_WITH_KEYS)
	evict = clean.read_and_process_evictions(EVICTIONS_FILE)
	rindex = clean.read_and_process_rindex(ZILLOW_FILE, ZILLOW_WITH_INC)
	cha_ev = merge_with_evict(cha, evict)
	cha_ev_r = merge_with_rindex(cha_ev, rindex)
	cha_ev_r.to_csv(CHA_WITH_EV_RINDEX)
	return cha_ev_r


def merge_with_evict(cha, evict):
	evict_to_merge = evict[evict.year == '2016']
	evict_to_merge = evict_to_merge[["GEOID", "year", "eviction-rate", 
										"eviction-filing-rate"]]
	merged_with_ev = pd.merge(cha.reset_index(), evict_to_merge, on="GEOID", 
							how="left")
	merged_with_ev.set_index("index", inplace=True)
	return merged_with_ev


def merge_with_rindex(cha, rindex):
	rindex_to_merge = rindex[["RegionID","2011-2015", "2015-2019"]]
	merged_with_rindex = pd.merge(cha, rindex_to_merge, on="RegionID", 
							how="left")
	return merged_with_rindex


if __name__ == '__main__':
	main()
