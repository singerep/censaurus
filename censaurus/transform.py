import matplotlib.pyplot as plt
from shapely import affinity, union_all
from typing import Dict, Tuple

from censaurus.dataset import ACS5
from censaurus.cdf import CensusGeoDataFrame


def _transform(geometry, scale, center, x_offset, y_offset, rotation):
    geometry = affinity.scale(geometry, scale, scale, scale, origin=center)
    geometry = affinity.rotate(geometry, rotation, origin=center)
    geometry = affinity.translate(geometry, x_offset, y_offset)
    return geometry

def _apply_transformation(row, state, scale, center, x_offset, y_offset, rotation):
    if row['state'] == state:
        row['geometry'] = _transform(row['geometry'], scale, center, x_offset, y_offset, rotation)
    return row

DEFAULT_CONTINENTAL_TRANSFORMATIONS = {
    '02': (0.4, 1000000, -5000000, 45),
    '15': (1, 4000000, 0, 0)
}
    
def continental(data: CensusGeoDataFrame, state_col: str = 'state', transformations: Dict[str, Tuple] = DEFAULT_CONTINENTAL_TRANSFORMATIONS) -> CensusGeoDataFrame:
    original_crs = data.crs
    data.to_crs(crs='EPSG:2163', inplace=True)
    for s, (scale, x_offset, y_offset, rotation) in transformations.items():
        centroid = union_all(data[data[state_col] == s]['geometry'].values).centroid
        data = data.apply(_apply_transformation, axis=1, args=(s, scale, centroid, x_offset, y_offset, rotation))

    data.to_crs(crs=original_crs, inplace=True)

    return data

acs = ACS5(census_api_key='11d46bc70e375d39b67b4b4919a0099934aecbc7')

# west = acs.areas.region('West')

states = acs.states(variables=['B01001_001E'], return_geometry=True)

states = continental(data=states)

states.plot()
plt.show()

# # for state, geom in zip(states['NAME'].values, states['geometry'].values):
# #     print(state)
# #     print(geom.bounds)
# #     print()