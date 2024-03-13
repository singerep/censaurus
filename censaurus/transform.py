import matplotlib.pyplot as plt
from shapely import affinity, union_all, Point
from typing import Dict, Tuple

from censaurus.dataset import ACS5
from censaurus.cdf import CensusGeoDataFrame


def _transform_geometry(geometry, scale, center, x_offset, y_offset, rotation):
    geometry = affinity.scale(geometry, scale, scale, scale, origin=center)
    geometry = affinity.rotate(geometry, rotation, origin=center)
    geometry = affinity.translate(geometry, x_offset, y_offset)
    return geometry

def _apply_transformation(row, state, scale, center, x_offset, y_offset, rotation):
    if row['state'] == state:
        row['geometry'] = _transform_geometry(row['geometry'], scale, center, x_offset, y_offset, rotation)
    return row

def _transform(data: CensusGeoDataFrame, region_col: str, transformations: Dict[str, Tuple]) -> CensusGeoDataFrame:
    for s, (scale, x_offset, y_offset, rotation) in transformations.items():
        centroid = union_all(data[data[region_col] == s]['geometry'].values).centroid
        data = data.apply(_apply_transformation, axis=1, args=(s, scale, centroid, x_offset, y_offset, rotation))

    return data

CONTINENTAL_EA_BELOW = {
    '02': (1, 600000, -5250000, 45),
    '15': (1, 5000000, -1100000, 45),
    '72': (1, -2500000, 300000, 0)
}

CONTINENTAL_SCALED_BELOW = {
    '02': (0.4, 700000, -4750000, 45),
    '15': (1, 5000000, -1100000, 45),
    '72': (1, -2500000, 300000, 0)
}

CONTINENTAL_EA_OUTSIDE = {
    '02': (1, 550000, -1250000, 45),
    '15': (1, 3100000, -75000, 45),
    '72': (1, -1300000, 200000, 0)
}

CONTINENTAL_SCALED_OUTSIDE = {
    '02': (0.4, 550000, -1750000, 45),
    '15': (1, 3100000, -75000, 45),
    '72': (1, -1300000, 200000, 0)
}


def continental(data: CensusGeoDataFrame, state_col: str = 'state', position: str = 'below', preserve_area: bool = False, custom_transformations: Dict[str, Tuple] = None) -> CensusGeoDataFrame:
    # TODO: add warning about equal area (equal area during scaling, but CRS may not be equal area)
    if custom_transformations:
        transformations = custom_transformations
    else:
        if position == 'below' and preserve_area is True:
            transformations = CONTINENTAL_EA_BELOW
        elif position == 'below' and preserve_area is False:
            transformations = CONTINENTAL_SCALED_BELOW
        elif position == 'outside' and preserve_area is True:
            transformations = CONTINENTAL_EA_OUTSIDE
        elif position == 'outside' and preserve_area is False:
            transformations = CONTINENTAL_SCALED_OUTSIDE
        else:
            raise ValueError('Must set position to either below or outside, and must set preserve_area to True or False, or you may use custom_transformations.')

    original_crs = data.crs
    data.to_crs(crs='EPSG:2163', inplace=True)

    data = _transform(data, state_col, transformations)

    data.to_crs(crs=original_crs, inplace=True)

    return data

def comparison(data: CensusGeoDataFrame, comparison_col: str):
    # TODO: figure out what to do if a geometry it wraps around
    comparison_groups = sorted(list(data[comparison_col].unique()))

    group_centroids = {}
    group_bounds = {}
    for g in comparison_groups:
        group_geometry = union_all(data[data[comparison_col] == g]['geometry'].values)
        group_centroids[g] = group_geometry.centroid
        group_bounds[g] = group_geometry.bounds

    anchor = comparison_groups[0]
    anchor_height = group_bounds[anchor][3] - group_bounds[anchor][1]
    anchor_width = group_bounds[anchor][2] - group_bounds[anchor][0]
    
    mid_height = group_bounds[anchor][1] + anchor_height/2
    right_edge = group_bounds[anchor][2]
    offset = anchor_width*0.1

    transformations = {}
    for g in comparison_groups[1:]:
        new_height = group_bounds[g][3] - group_bounds[g][1]
        scale = anchor_height/new_height

        new_width = (group_bounds[g][2] - group_bounds[g][0])*scale

        move_x = (right_edge + offset + new_width/2) - (group_bounds[g][0] + (new_width/scale)/2)
        move_y = mid_height - (group_bounds[g][1] + new_height/2)

        transformations[g] = (scale, move_x, move_y, 0)

        right_edge = right_edge + offset + new_width

    data = _transform(data, comparison_col, transformations)

    return data