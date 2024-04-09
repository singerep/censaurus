from __future__ import annotations

from censaurus.geography import Geography
from censaurus.variable import Variable, VariableCollection
from censaurus.tiger import US_CARTOGRAPHIC, AreaCollection, _water_within_geometry

from pandas import DataFrame, Series
from geopandas import GeoDataFrame, GeoSeries
from geopandas.base import is_geometry_type
from shapely import union_all
from shapely.geometry import box, MultiPolygon, Polygon
from typing import Union, overload, Literal, Dict, Tuple
from pandas._typing import Axis, Level, IgnoreRaise, Renamer

from censaurus.utils.rename import CensusRenamer
from censaurus.utils.recode import StateRecoder
from censaurus.utils.regroup import CensusRegrouper
from censaurus.utils.transform_geometry import make_contiguous, compare_regions


# Logic for subclassing heavily inspired by the Geopandas library (GeoDataFrame and GeoSeries)
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
    def _constructor(self, *args, **kwargs) -> CensusDataFrame:
        def _cdf_constructor(*args, **kwargs):
            kwargs.update({'geography': self.geography, 'variables': self.variables})
            cdf = CensusDataFrame(*args, **kwargs)
            return cdf
        
        return _cdf_constructor

    @property
    def _constructor_sliced(self, *args, **kwargs) -> CensusSeries:
        def _cdf_constructor_sliced(*args, **kwargs):
            name = kwargs.pop("name", None)

            if name in self._variable_map:
                srs = CensusSeries(*args, **kwargs, variable=self._variable_map[name])
            else:
                srs = Series(*args, **kwargs)
            return srs

        return _cdf_constructor_sliced

    @overload
    def rename(self, census_renamer: CensusRenamer, inplace: bool = False) -> DataFrame | None:
        ...

    @overload
    def rename(
        self,
        mapper: Renamer | None = ...,
        *,
        index: Renamer | None = ...,
        columns: Renamer | None = ...,
        axis: Axis | None = ...,
        copy: bool | None = ...,
        inplace: Literal[True],
        level: Level = ...,
        errors: IgnoreRaise = ...,
    ) -> None:
        ...

    @overload
    def rename(
        self,
        mapper: Renamer | None = ...,
        *,
        index: Renamer | None = ...,
        columns: Renamer | None = ...,
        axis: Axis | None = ...,
        copy: bool | None = ...,
        inplace: Literal[False] = ...,
        level: Level = ...,
        errors: IgnoreRaise = ...,
    ) -> DataFrame:
        ...

    @overload
    def rename(
        self,
        mapper: Renamer | None = ...,
        *,
        index: Renamer | None = ...,
        columns: Renamer | None = ...,
        axis: Axis | None = ...,
        copy: bool | None = ...,
        inplace: bool = ...,
        level: Level = ...,
        errors: IgnoreRaise = ...,
    ) -> DataFrame | None:
        ...

    def rename(self, *args, **kwargs):
        inplace = kwargs.get('inplace', False)
        census_renamer = kwargs.get('census_renamer', None)
        columns = kwargs.get('columns', None)
        mapper = kwargs.get('mapper', None)
        axis = kwargs.get('axis', None)

        census_renamers = [a for a in args if isinstance(a, CensusRenamer)]
        if len(census_renamers) > 0:
            if len(census_renamers) > 1:
                raise ValueError('Can only pass in one CensusRenamer object')
            census_renamer = census_renamers[0]
            
        if census_renamer:
            return census_renamer.rename(self, inplace=inplace)

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

    def regroup(self, census_regrouper: CensusRegrouper, inplace: bool = False) -> None:
        if inplace:
            census_regrouper.regroup(self, inplace=True)
        else:
            return census_regrouper.regroup(self, inplace=False)

    def recode(self, format: Literal['FIPS', 'FIPS_PADDED', 'ABBR', 'NAME', 'GNIS', 'GNIS_PADDED'], inplace: bool = False) -> None:
        """
        An object that handles recoding state to various formats. The available formats are:
        
            + ``FIPS``: FIPS codes with no zero-padding (1, 2, 4, etc.)
            + ``FIPS_PADDED``: FIPS codes with two-digit zero-padding (01, 02, 04, etc.)
            + ``ABBR``: state abbreviations (AL, AK, AZ, etc.)
            + ``NAME``: full state names (Alabama, Alaska, Arizona, etc.)
            + ``GNIS``: Geographic Names Information System codes (1779775, 1785533, 1779777 etc.)
            + ``GNIS_PADDED``: Geographic Names Information System codes with seven-digit zero-padding (1779775, 1785533, 1779777 etc.)

        """
        if format not in ['FIPS', 'FIPS_PADDED', 'ABBR', 'NAME', 'GNIS', 'GNIS_PADDED']:
            joined_types = ', '.join(['FIPS', 'FIPS_PADDED', 'ABBR', 'NAME', 'GNIS', 'GNIS_PADDED'])
            raise ValueError(f'format must be one of {joined_types}')
        
        recorder = StateRecoder()
        if inplace:
            recorder._to_new(self, new_type=format, inplace=True)
        else:
            return recorder._to_new(self, new_type=format, inplace=False)
    

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
    def _constructor(self, *args, **kwargs) -> Union[CensusGeoDataFrame, CensusDataFrame]:
        def _cdf_constructor(*args, **kwargs):
            kwargs.update({'geography': self.geography, 'variables': self.variables, 'areas': self.areas})
            df = CensusGeoDataFrame(*args, **kwargs)
            geometry_cols_mask = df.dtypes == "geometry"
            if len(geometry_cols_mask) == 0 or geometry_cols_mask.sum() == 0:
                df = CensusDataFrame(df, geography=self.geography, variables=self.variables)

            return df
        
        return _cdf_constructor

    @property
    def _constructor_sliced(self, *args, **kwargs) -> Union[CensusSeries, GeoSeries]:
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

    def clip_to_cb(self, inplace: bool = False) -> Union[None, CensusGeoDataFrame]:
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

    def remove_water(self, min_area: float = None, min_area_pct: float = None, top_n: int = None, top_pct: float = None, keep_internal: bool = False, inplace: bool = False) -> CensusGeoDataFrame | None:
        geometry = union_all(self['geometry'].values)

        water_geom = _water_within_geometry(geometry=geometry, min_area=min_area, min_area_pct=min_area_pct, top_n=top_n, top_pct=top_pct, keep_internal=keep_internal)

        if inplace:
            if water_geom is not None:
                self['geometry'] = self.difference(water_geom)
        else:
            c = self.copy()
            c['geometry'] = c.difference(water_geom)
            return c

    def make_contiguous(self, state_col: str = 'state', geometry_col: bool = 'geometry', position: str = 'below', preserve_area: bool = False, custom_transformations: Dict[str, Tuple] = None, inplace: bool = False) -> CensusGeoDataFrame | None:
        return make_contiguous(self, geometry_col=geometry_col, state_col=state_col, position=position, preserve_area=preserve_area, custom_transformations=custom_transformations, inplace=inplace)

    def compare_regions(self, comparison_col: str, geometry_col: bool = 'geometry', inplace: bool = False) -> CensusGeoDataFrame | None:
        return compare_regions(self, comparison_col=comparison_col, geometry_col=geometry_col, inplace=inplace)