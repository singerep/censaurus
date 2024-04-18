from __future__ import annotations

from shapely import affinity, union_all, difference
from geopandas import GeoDataFrame
from typing import Dict, Tuple, Union, Literal, List
from pandas import Series
from pandas.api.types import is_numeric_dtype

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
    '02': (0.35, 700000, -4750000, 45),
    '15': (1, 4800000, -1100000, 45),
    '72': (1, -2500000, 300000, 0)
}

CONTINENTAL_EA_OUTSIDE = {
    '02': (1, 550000, -1250000, 45),
    '15': (1, 3100000, -75000, 45),
    '72': (1, -1300000, 200000, 0)
}

CONTINENTAL_SCALED_OUTSIDE = {
    '02': (0.35, 550000, -1750000, 45),
    '15': (1, 3100000, -75000, 45),
    '72': (1, -1300000, 200000, 0)
}

def recompose_states(data: Union[censaurus.CensusGeoDataFrame, GeoDataFrame], state_col: str = 'state', geometry_col: str = 'geometry', position: str = Literal['below', 'outside'], preserve_area: bool = False, inplace: bool = False) -> Union[censaurus.CensusGeoDataFrame, GeoDataFrame] | None:
    """
    Shift the geographies of Alaska, Hawaii, and Puerto Rico so that they appear
    nearer to the contiguous United States.
    
    Inspired by Kyle Walker's `shift_geometry <https://github.com/walkerke/tigris/blob/master/R/shift_geometry.R>`_
    function in the ``tigris`` R package and the `geoAlbersUSA <https://d3js.org/d3-geo/conic#geoAlbersUsa>`_
    projection in the ``d3`` library.

    Parameters
    ==========
    state_col : :obj:`str` = 'state'
        The dataset's state column. Should be in padded FIPS format. See the
        :meth:`.CensusDataFrame.recode` for automatically recoding your data to this
        format.
    geometry_col : :obj:`str` = 'geometry'
        The dataset's geometry column.
    position : :obj:`str` = Literal['below', 'outside']
        Determines whether Alaska, Hawaii, and Puerto Rico should be position below
        our outside of the contiguous United States.
    preserve_area : :obj:`bool` = False
        Determines whether Alaska, Hawaii, and Puerto Rico should be rescaled,
        or whether their original area should be maintained.
    inplace : :obj:`bool` = False
        Determines whether to modify the original dataset or create and return a new 
        one.
    """
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
    """
    Transform a dataset's geometries so that two or more geographic regions can be
    visually compared.
    
    Rows are grouped by the specified ``comparison_col``, and geometries are
    repositioned and rescaled so that groups appear in a horizontal line.

    Parameters
    ==========
    comparison_col : :obj:`str`
        The column used to partition rows up into groups.
    geometry_col : :obj:`str` = 'geometry'
        The dataset's geometry column.
    inplace : :obj:`bool` = False
        Determines whether to modify the original dataset or create and return a new 
        one.
    """
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

def _crosswalk_by_area(source: Union[censaurus.CensusGeoDataFrame, GeoDataFrame], destination: Union[censaurus.CensusGeoDataFrame, GeoDataFrame], count_columns: List[str]) -> GeoDataFrame:    
    S = source.copy()
    S = S[count_columns + [S.geometry.name]]
    S['S_area'] = S.area
    
    destination['D_id'] = destination.index
    D = destination.copy()

    X : GeoDataFrame = S.overlay(right=D, how='intersection', keep_geom_type=False) # removes areas that are outside both geometries; this makes sense because no data!
    X['weight'] = X.area/X['S_area']
    X[count_columns] = X[count_columns].multiply(X['weight'], axis="index")

    D_prime = X.groupby('D_id', as_index=False)[count_columns].sum()
    destination_prime = destination.merge(D_prime, on='D_id', how='inner').drop(labels=['D_id'], axis=1)

    return destination_prime

def _crosswalk_by_weight(source: Union[censaurus.CensusGeoDataFrame, GeoDataFrame], destination: Union[censaurus.CensusGeoDataFrame, GeoDataFrame], weight_col: str, count_columns: List[str]) -> GeoDataFrame:    
    source['S_id'] = source.index
    S = source.copy()
    S = S[count_columns + [S.geometry.name] + ['S_id']]
    S['S_area'] = S.area
    
    destination['D_id'] = destination.index
    D = destination.copy()
    D['D_area'] = D.area

    X : GeoDataFrame = S.overlay(right=D, how='intersection', keep_geom_type=False) # removes areas that are outside both geometries; this makes sense because no data!
    X['X_area'] = X.area
    X['weight'] = X[weight_col]*(X['X_area']/X['D_area'])

    S_weights = X.groupby('S_id', as_index=False)[['weight', 'X_area']].sum().rename(columns={'weight': 'S_weight', 'X_area': 'S_weighted_area'})

    X = X.merge(S_weights, on='S_id', how='inner')
    X['weight'] = (X['weight']/X['S_weight'])*(X['S_weighted_area']/X['S_area'])

    X[count_columns] = X[count_columns].multiply(X['weight'], axis="index")

    D_prime = X.groupby('D_id', as_index=False)[count_columns].sum()
    destination_prime = destination.merge(D_prime, on='D_id', how='inner').drop(labels=['D_id'], axis=1)

    return destination_prime
    

def crosswalk(source: Union[censaurus.CensusGeoDataFrame, GeoDataFrame], destination: Union[censaurus.CensusGeoDataFrame, GeoDataFrame], weight_col: str = None, count_columns: List[str] = None, count_placement: Literal['right', 'left'] = 'right') -> Union[censaurus.CensusGeoDataFrame, GeoDataFrame]:
    """
    Estimate count variables (such as total population, number of homeowners, etc.) for 
    geographic units in a ``destination`` dataset by computing a crosswalk from
    geographic units in a ``source`` dataset where those count variables are specified.

    The general framework to compute crosswalks is as follows.

    **Setup and notation**

    * We are given the source layer :math:`S` (indexed by :math:`i`) and the destination 
      layer :math:`D` (indexed by :math:`j`).
    
    * We use :math:`C_u` to refer to the vector of count variables corresponding to
      a specified geographic unit :math:`u`. Note: the count vectors are already 
      specified for each source unit :math:`S_i`, while our goal is to estimate the 
      count vectors for each destination unit :math:`D_j`.

    * *Optional*: We use :math:`W_u` to refer to the weight (such as population count)
      of a specified geographic unit :math:`W_u`. If a weighted crosswalk is desired, 
      the weight of each destination unit :math:`W_{D_j}` must be specified.

    * We use :math:`\| u \|` to denote the area of a specific geographic unit :math:`u`.

    **Algorithm**

    #. Intersect the source layer :math:`S` with the destination layer :math:`D` to
       create a cross layer :math:`X` (indexed by :math:`i, j`) where :math:`X_{i, j}` 
       is the intersection between :math:`S_i` and :math:`D_j`.

    #. Estimate :math:`\\hat{C}_{X_{i,j}}` for each unit in the cross layer.

       If weights are not supplied, :math:`\\hat{C}_{X_{i,j}}` is computed using a naive
       areal approach.

         .. math::

            \\hat{C}_{X_{i,j}} = \\frac{\| X_{i,j} \|}{\| S_i \|} \\cdot C_{S_i}

       If weights are supplied, :math:`\\hat{C}_{X_{i,j}}` is computed using a weighted
       approach.

         .. math::

            \\hat{C}_{X_{i,j}} = \\frac{W_{X_{i,j}}}{\\sum\\limits_{X_{i',j'} \in S_i} W_{X_{i',j'}}} \\cdot \\frac{\\sum\\limits_{X_{i',j'} \in S_i} \| X_{i',j'} \|}{\| S_i \|} \\cdot C_{S_i}

       where

         .. math::

            W_{X_{i,j}} = \\frac{\| X_{i,j} \|}{\| D_j \|} \\cdot W_{D_j}

    #. Estimate :math:`\\hat{C}_{D_{j}}` for each unit in the destination layer by
       aggregating estimated count vectors in the cross layer.

         .. math::

            \\hat{C}_{D_j} = \\sum\\limits_{X_{i,j} \in D_j} \\hat{C}_{X_{i,j}}

    Parameters
    ==========
    source : :class:`.CensusGeoDataFrame` or :obj:`.GeoDataFrame`
        The source geographic dataset. This dataset should have columns corresponding
        to various count variables that should be crosswalked.
    destination : :class:`.CensusGeoDataFrame` or :obj:`.GeoDataFrame`
        The destination geographic dataset. If ``weight_col`` is specified, this dataset
        should have a column corresponding to the specified variable.
    weight_col : :obj:`str` = None
        If a ``weight_col`` is supplied, the crosswalk is computed using the weighted
        approach. Otherwise, the crosswalk is computed using the naive areal approach.
    count_columns : list of :obj:`str` = None
        If supplied, only the specified count variables are crosswalked. If 
        ``count_columns`` is not supplied, the count variables to crosswalk are assumed
        to be all numeric columns in the ``source`` dataset (if the ``source`` is a 
        :obj:`.GeoDataFrame`) or all numeric columns that have an associated 
        :class:`.Variable` (if the ``source`` is a :class:`.CensusGeoDataFrame`).
    count_placement : :obj:`Literal['right', 'left']` = 'right'
        Determines whether or not to place the new estimated count variables on the
        right side or left side of the ``destination`` dataset.
    """
    if source.crs != destination.crs:
        raise ValueError('The source and to datasets should have the same coordinate reference system!')

    if count_placement not in ['right', 'left']:
        raise ValueError("The count_placement must be either 'right' or 'left'.")

    source_shape = union_all(source.geometry.values)
    destination_shape = union_all(destination.geometry.values)
    if not source_shape.contains(destination_shape):
        d = difference(destination_shape, source_shape)
        print(f'Warning: {round(d.area/destination_shape.area, 2)}% of your destination dataset lies outside of your source dataset. This area will be removed during the crosswalk.')

    if count_columns is None:
        if isinstance(source, censaurus.CensusGeoDataFrame):
            count_columns = [c for c in source.columns if is_numeric_dtype(source[c]) and c in source._variable_map]
        else:
            count_columns = [c for c in source.columns if is_numeric_dtype(source[c])]
        
        # TODO: add a message here about what columns are selected, etc.

    if weight_col:
        destination_prime = _crosswalk_by_weight(source=source, destination=destination, weight_col=weight_col, count_columns=count_columns)
    else:
        destination_prime = _crosswalk_by_area(source=source, destination=destination, count_columns=count_columns)

    if count_placement == 'left':
        col_order = count_columns + [c for c in destination_prime.columns if c not in count_columns]
    else:
        col_order = [c for c in destination_prime.columns if c not in count_columns] +  count_columns

    if isinstance(source, censaurus.CensusDataFrame):
        if isinstance(destination, censaurus.CensusDataFrame):
            destination_prime = censaurus.CensusDataFrame(destination_prime, geography=destination.geography, variables=source.variables)
        else:
            destination_prime = censaurus.CensusDataFrame(destination_prime, geography=None, variables=source.variables)

    return destination_prime[col_order]