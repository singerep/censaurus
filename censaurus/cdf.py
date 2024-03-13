from censaurus.geography import Geography
from censaurus.variable import Variable, VariableCollection
from censaurus.tiger import US_CARTOGRAPHIC, AreaCollection, _water_within_geometry

from pandas import DataFrame, Series
from geopandas import GeoDataFrame, GeoSeries
from geopandas.base import is_geometry_type
from shapely import union_all
from shapely.geometry import box, MultiPolygon, Polygon
from typing import Union

# Logic for subclassing heavily inspired by the Geopandas library


class CensusDataFrame(DataFrame):
    """
    A CensusDataFrame object is a pandas.DataFrame that stores additional information
    about the variables and geography associated with the data.
    """

    _metadata =  ["geography", "variables"]

    def __init__(self, data=None, *args, geography: Geography, variables: VariableCollection, **kwargs):
        super().__init__(data, *args, **kwargs)
        self.geography = geography
        self.variables = variables
        self._variable_map = {c: self.variables.get(c) for c in self.columns}

    def __setattr__(self, attr, val):
        if attr in ['geography', 'variables', 'areas', '_variable_map']:
            object.__setattr__(self, attr, val)
        else:
            super().__setattr__(attr, val)

    @property
    def _constructor(self, *args, **kwargs) -> 'CensusDataFrame':
        def _cdf_constructor(*args, **kwargs):
            kwargs.update({'geography': self.geography, 'variables': self.variables})
            cdf = CensusDataFrame(*args, **kwargs)
            return cdf
        
        return _cdf_constructor

    @property
    def _constructor_sliced(self, *args, **kwargs) -> 'CensusSeries':
        def _cdf_constructor_sliced(*args, **kwargs):
            name = kwargs.pop("name", None)

            if name in self._variable_map:
                srs = CensusSeries(*args, **kwargs, variable=self._variable_map[name])
            else:
                srs = Series(*args, **kwargs)
            return srs

        return _cdf_constructor_sliced

    def rename(self, *args, **kwargs):
        inplace = kwargs.get('inplace', False)
        columns = kwargs.get('columns', None)
        mapper = kwargs.get('mapper', None)
        axis = kwargs.get('axis', None)

        cdf = super().rename(*args, **kwargs)

        if inplace:
            new_vmap = self._variable_map
        else:
            new_vmap = cdf._variable_map

        if mapper and axis == 1:
            columns = mapper
        
        if columns:
            for k, v in list(new_vmap.items()):
                if k in columns:
                    new_vmap[columns[k]] = v
                    del new_vmap[k]

        return cdf

class CensusSeries(Series):
    def __init__(self, data = None, index = None, variable: Variable = None, **kwargs):
        super().__init__(data, index=index, **kwargs)

        self.variable = variable

    @property
    def _constructor(self, *args, **kwargs) -> 'CensusSeries':
        def _cdf_series_constructor(*args, **kwargs):
            kwargs.update({'variable': self.variable})
            cs = CensusSeries(*args, **kwargs)
            return cs
        
        return _cdf_series_constructor


class CensusGeoDataFrame(GeoDataFrame, CensusDataFrame):
    _metadata =  ["geography", "variables"]

    def __init__(self, data=None, *args, geography: Geography, variables: VariableCollection, areas: AreaCollection, **kwargs):
        kwargs.update({'geography': geography, 'variables': variables})
        super().__init__(data, *args, **kwargs)

        self.areas = areas

    @property
    def _constructor(self, *args, **kwargs) -> Union['CensusGeoDataFrame', CensusDataFrame]:
        def _cdf_constructor(*args, **kwargs):
            kwargs.update({'geography': self.geography, 'variables': self.variables, 'areas': self.areas})
            df = CensusGeoDataFrame(*args, **kwargs)
            geometry_cols_mask = df.dtypes == "geometry"
            if len(geometry_cols_mask) == 0 or geometry_cols_mask.sum() == 0:
                df = CensusDataFrame(df, geography=self.geography, variables=self.variables)

            return df
        
        return _cdf_constructor

    @property
    def _constructor_sliced(self, *args, **kwargs) -> Union['CensusSeries', GeoSeries]:
        def _cdf_constructor_sliced(*args, **kwargs):
            srs = Series(*args, **kwargs)
            is_row_proxy = srs.index is self.columns
            if is_geometry_type(srs) and not is_row_proxy:
                srs = GeoSeries(srs)
            else:
                name = kwargs.pop("name", None)
                if name in self._variable_map:
                    srs = CensusSeries(*args, **kwargs, variable=self._variable_map[name])
            
            return srs

        return _cdf_constructor_sliced

    def clip_to_cb(self, inplace: bool = True) -> Union[None, 'CensusGeoDataFrame']:
        if US_CARTOGRAPHIC._attributes_are_set is False:
            US_CARTOGRAPHIC._set_attributes()
        
        intersections = self.intersection(other=US_CARTOGRAPHIC.geometry)

        intersecting_mask = intersections.area/self.area > 0

        if inplace:
            self['geometry'] = intersections
            self = self[intersecting_mask]
            self.reset_index(inplace=True)
        else:
            c = self.copy()
            c['geometry'] = intersections
            c = c[intersecting_mask]
            c.reset_index(inplace=True)
            return c

    def remove_water(self, min_area: float = None, min_area_pct: float = None, top_n: int = None, top_pct: float = None, keep_internal: bool = False, inplace: bool = True) -> Union[None, 'CensusGeoDataFrame']:
        geometry = union_all(self['geometry'].values)

        water_geom = _water_within_geometry(geometry=geometry, min_area=min_area, min_area_pct=min_area_pct, top_n=top_n, top_pct=top_pct, keep_internal=keep_internal)

        if inplace:
            if water_geom is not None:
                self['geometry'] = self.difference(water_geom)
        else:
            c = self.copy()
            c['geometry'] = c.difference(water_geom)
            return c