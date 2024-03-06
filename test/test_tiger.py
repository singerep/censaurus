from unittest import TestCase, main
from pandas import DataFrame
from geopandas import GeoDataFrame
from shapely import union_all, difference

from censaurus.tiger import Area, US_CARTOGRAPHIC, Layer, AreaCollection, shapely_to_esri_json
from censaurus.api import TIGERClient
from censaurus.dataset import ACS5, Decennial

import time
import matplotlib.pyplot as plt
acs = ACS5(census_api_key='11d46bc70e375d39b67b4b4919a0099934aecbc7')

ma = acs.areas.state('MA', cb=False)
ny = acs.areas.state('NY', cb=False)
# nj = acs.areas.state('NJ')
# pa = acs.areas.state('PA')
# ct = acs.areas.state('CT')
# ri = acs.areas.state('RI')
# vt = acs.areas.state('VT')
# nh = acs.areas.state('nh')
# me = acs.areas.state('me')
# states = [ma, ny, nj, pa, ct, ri, vt, nh, me]
# northeast = acs.areas.region('Northeast')

# print(cities_ma)

# tracts_ma = acs.tracts(within=ma, variables=['B01001_001E'], area_threshold=0.01)
# tracts_ny = acs.tracts(within=ny, variables=['B01001_001E'], area_threshold=0.01)
tracts_ma_ny = acs.tracts(within=[ma, ny], variables=['B01001_001E'], area_threshold=0.01, return_geometry=True)

# print(tracts_ma['state'].value_counts())
# print(tracts_ny['state'].value_counts())
print(tracts_ma_ny['state'].value_counts())

tracts_ma_ny.plot()
plt.show()

# tracts_combined__states = acs.tracts(within=states, variables=['B01001_001E'], area_threshold=0.01)
# tracts_region = acs.tracts(within=northeast, variables=['B01001_001E'], area_threshold=0.01)

# missing = list(set(tracts_combined__states['tract'].values) - set(tracts_region['tract'].values))

# tracts_missing = tracts_combined__states[tracts_combined__states['tract'].isin(missing)]
# print(tracts_missing)
# print(tracts_missing['state'].value_counts())

# tracts_missing.plot()
# plt.show()


# print(tracts_combined__states['state'].value_counts())
# print(tracts_combined__states['state'].value_counts())
# print(tracts_region['state'].value_counts())

# tracts.plot()
# plt.show()

# tracts.plot(column="state")
# plt.show()
# tracts.plot

# print(tracts['state'].value_counts())

# class TIGERTest(TestCase):
#     @classmethod
#     def setUpClass(cls):
#         cls.area_collection = AreaCollection()
#         cls.tiger_client = TIGERClient()

#     def test_area_inits(self):
#         montgomery = Area.from_tiger(geo_id='24031', layer_id='82', layer_name='States', tiger_client=self.tiger_client)
#         montgomery._set_attributes()
#         self.assertEqual(montgomery.name, 'Montgomery County')

#         US_CARTOGRAPHIC._set_attributes()
#         self.assertEqual(US_CARTOGRAPHIC.name, 'United States (cartographic boundary)')

#     def test_get_features(self):
#         state_layer = self.area_collection.get_layer('States')
#         states = state_layer.get_features()
#         self.assertIsInstance(states, GeoDataFrame)
#         self.assertEqual(len(states), 56)

#     def test_area_search(self):
#         la = self.area_collection.county('Los Angeles County, California')
#         self.assertEqual(la.name, 'Los Angeles County')

#         la = self.area_collection.county('Los Angeles County')
#         self.assertEqual(la.name, 'Los Angeles County')

#         la = self.area_collection.county('Los Angeles')
#         self.assertEqual(la.name, 'Los Angeles County')

#         md = self.area_collection.state('Maryland')
#         self.assertEqual(md.name, 'Maryland')

#         md = self.area_collection.state('MD')
#         self.assertEqual(md.name, 'Maryland')

#         md = self.area_collection.state(geoid=24)
#         self.assertEqual(md.name, 'Maryland')

#         ny01 = self.area_collection.congressional_district('NY-01')
#         self.assertEqual(ny01.name, 'Congressional District 1')

#         ny01 = self.area_collection.congressional_district('New York 1')
#         self.assertEqual(ny01.name, 'Congressional District 1')


# if __name__ == "__main__":
#     main()

"""
state
36    4934
42    3029
34    1975
25    1492
09     797
23     377
33     309
50     169
44     143
13225
Name: count, dtype: int64
state
36    5411
42    3446
34    2181
25    1620
09     883
23     407
33     350
50     193
44     250
14741
"""