from __future__ import annotations

from shapely import affinity, union_all
from geopandas import GeoDataFrame
from typing import Dict, Tuple, Union
from geopandas import GeoDataFrame

import censaurus


def _transform_geometry(geometry, scale, center, x_offset, y_offset, rotation):
    geometry = affinity.scale(geometry, scale, scale, scale, origin=center)
    geometry = affinity.rotate(geometry, rotation, origin=center)
    geometry = affinity.translate(geometry, x_offset, y_offset)
    return geometry

def _apply_transformation(row, geometry_col, region_col, region, scale, center, x_offset, y_offset, rotation):
    if row[region_col] == region:
        row[geometry_col] = _transform_geometry(row[geometry_col], scale, center, x_offset, y_offset, rotation)
    return row

def _transform(data: Union[censaurus.CensusGeoDataFrame, GeoDataFrame], geometry_col: str, region_col: str, transformations: Dict[str, Tuple]) -> Union[censaurus.CensusGeoDataFrame, GeoDataFrame]:    
    for r, (scale, x_offset, y_offset, rotation) in transformations.items():
        centroid = union_all(data[data[region_col] == r][geometry_col].values).centroid
        data = data.apply(_apply_transformation, axis=1, args=(geometry_col, region_col, r, scale, centroid, x_offset, y_offset, rotation))

    return data

CONTINENTAL_EA_BELOW = {
    '02': (1, 600000, -5250000, 45),
    '15': (1, 5000000, -1100000, 45),
    '72': (1, -2500000, 300000, 0)
}

CONTINENTAL_SCALED_BELOW = {
    '02': (0.4, 700000, -4750000, 45),
    '15': (1, 4800000, -1100000, 45),
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

def make_contiguous(data: Union[censaurus.CensusGeoDataFrame, GeoDataFrame], state_col: str = 'state', geometry_col: str = 'geometry', position: str = 'below', preserve_area: bool = False, custom_transformations: Dict[str, Tuple] = None, inplace: bool = False) -> Union[censaurus.CensusGeoDataFrame, GeoDataFrame] | None:
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

    if inplace is False:
        data = data.copy()

    data.to_crs(crs='EPSG:2163', inplace=True)

    data[geometry_col] = _transform(data, geometry_col, state_col, transformations)[geometry_col]

    data.to_crs(crs=original_crs, inplace=True)

    if inplace:
        return data
    

def compare_regions(data: Union[censaurus.CensusGeoDataFrame, GeoDataFrame], comparison_col: str, geometry_col: str, inplace: bool = False) -> Union[censaurus.CensusGeoDataFrame, GeoDataFrame] | None:
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
    
    bottom_edge = group_bounds[anchor][3]
    right_edge = group_bounds[anchor][2]
    offset = anchor_width*0.1

    transformations = {}
    for g in comparison_groups[1:]:
        g_height = group_bounds[g][3] - group_bounds[g][1]
        g_width = group_bounds[g][2] - group_bounds[g][0]

        g_centroid_x = group_centroids[g].x
        g_centroid_y = group_centroids[g].y

        g_centroid_x_prop = ((g_centroid_x - group_bounds[g][0])/g_width)
        g_centroid_y_prop = ((g_centroid_y - group_bounds[g][3])/g_height)

        scale = anchor_height/g_height

        g_height_scaled = g_height*scale
        g_width_scaled = g_width*scale

        g_centroid_x_adj = right_edge + offset + g_centroid_x_prop*g_width_scaled
        g_centroid_y_adj = bottom_edge + g_centroid_y_prop*g_height_scaled

        move_x = g_centroid_x_adj - g_centroid_x
        move_y = g_centroid_y_adj - g_centroid_y

        transformations[g] = (scale, move_x, move_y, 0)

        right_edge += offset + g_width_scaled

    if inplace is False:
        data = data.copy()

    data[geometry_col] = _transform(data, geometry_col, comparison_col, transformations)[geometry_col]

    if inplace:
        return data