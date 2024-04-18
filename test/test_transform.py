from shapely.geometry import Polygon
import geopandas
import matplotlib.pyplot as plt
from censaurus.dataset import ACS5
import numpy as np

from censaurus.utils.transform_geometry import crosswalk

# polys1 = geopandas.GeoSeries([Polygon([(0,0), (2,0), (2,2), (0,2)]),
#                               Polygon([(0,2), (2,2), (2,4), (0,4)]),
#                               Polygon([(2,0), (4,0), (4,2), (2,2)]),
#                               Polygon([(2,2), (4,2), (4,4), (2,4)])])

# polys2 = geopandas.GeoSeries([Polygon([(0,0), (0,1), (1,0)]),
#                               Polygon([(1,0), (0,1), (0,2), (2,0)]),
#                               Polygon([(2,0), (0,2), (0,3), (3,0)]),
#                               Polygon([(3,0), (0,3), (0,4), (4,0)]),
#                               Polygon([(4,0), (0,4), (1,4), (4,1)]),
#                               Polygon([(4,1), (1,4), (2,4), (4,2)]),
#                               Polygon([(4,2), (2,4), (3,4), (4,3)]),
#                               Polygon([(4,3), (3,4), (4,4)])])

# df1 = geopandas.GeoDataFrame({
#     'geometry': polys1, 
#     'pop': [100, 500, 200, 30],
#     'homes': [50, 250, 100, 15]
# })

# df2 = geopandas.GeoDataFrame({
#     'geometry': polys2,
#     'name': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
# })

# x = crosswalk_by_area(df1, df2)
# print(x)


# overlay : geopandas.GeoDataFrame = df1.overlay(df2, how='intersection')
# overlay['atom_area'] = overlay.area
# overlay['atom_area_1'] = overlay['atom_area']/overlay['area_1']
# overlay['atom_area_2'] = overlay['atom_area']/overlay['area_2']


acs = ACS5()
ri = acs.areas.state('MA')

counties = acs.tracts(within=ri, variables=acs.variables.filter_by_group('B03002'), return_geometry=True)
counties.recode_states(format='ABBR', inplace=True)
zctas = acs.ZCTAs(within=ri, variables={'B01001_001E': 'total_pop', 'B03002_003E': 'true_white'}, return_geometry=True)

x = crosswalk(counties, zctas, weight_col='total_pop', count_placement='right')

print(np.corrcoef(x['true_white'], x['B03002_003E']))

plt.scatter(x['true_white'], x['B03002_003E'])
plt.axline([0, 0], [1, 1])
plt.show()

# print(counties)
# print(zctas)

# overlay = counties.overlay(zctas, how='intersection')

# ax = overlay.plot(alpha=0.5, cmap='tab20')

# counties.plot(ax=ax, facecolor='none', edgecolor='red', linewidth=2)

# zctas.plot(ax=ax, facecolor='none', edgecolor='blue')

# plt.show()