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
from censaurus.utils.transform_geometry import recompose_states, compare_regions


# Logic for subclassing heavily inspired by the Geopandas library (GeoDataFrame and GeoSeries)
class CensusDataFrame(DataFrame):
    """
    An adapted :obj:`pandas.DataFrame` object that stores additional information
    about the variables and geography associated with the data.

    Attributes
    ==========
    geography : :class:`.Geography`
        The geography level associated with the :class:`.CensusDataFrame`.
    variables : :class:`.VariableCollection`
        The variable collection associated with the :class:`.CensusDataFrame`.
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

    def rename_variables(self, census_renamer: CensusRenamer, inplace: bool = False) -> DataFrame | None:
        """
        Rename variables according to a :class:`.CensusRenamer` object.

        Parameters
        ==========
        census_renamer : :class:`.CensusRenamer`
            The CensusRenamer to use to carry out the renamings. Read the
            :class:`.CensusRenamer` documentation for more information.
        inplace : :obj:`bool` = False
            Determines whether to modify the original CensusDataFrame or create
            and return a new one.
        """
        if inplace:
            census_renamer.rename_variables(self, inplace=True)
        else:
            return census_renamer.rename_variables(self, inplace=False)

    def regroup_variables(self, census_regrouper: CensusRegrouper, inplace: bool = False) -> None:
        """
        Regroup variables according to a :class:`.CensusRegrouper` object.

        Parameters
        ==========
        census_regrouper : :class:`.CensusRegrouper`
            The CensusRegrouper to use to carry out the regroupings. Read the
            :class:`.CensusRegrouper` documentation for more information.
        inplace : :obj:`bool` = False
            Determines whether to modify the original CensusDataFrame or create
            and return a new one.
        """
        if inplace:
            census_regrouper.regroup_variables(self, inplace=True)
        else:
            return census_regrouper.regroup_variables(self, inplace=False)

    def recode_states(self, format: Literal['FIPS', 'FIPS_PADDED', 'ABBR', 'NAME', 'GNIS', 'GNIS_PADDED'], inplace: bool = False) -> None:
        """
        Recode state labels to various formats.

        Parameters
        ==========
        format : :obj:`Literal['FIPS', 'FIPS_PADDED', 'ABBR', 'NAME', 'GNIS', 'GNIS_PADDED']`
            The desired new label format. Format options are:

            + ``FIPS``: FIPS codes with no zero-padding (1, 2, 4, etc.)
            + ``FIPS_PADDED``: FIPS codes with two-digit zero-padding (01, 02, 04, etc.)
            + ``ABBR``: state abbreviations (AL, AK, AZ, etc.)
            + ``NAME``: full state names (Alabama, Alaska, Arizona, etc.)
            + ``GNIS``: Geographic Names Information System codes (1779775, 1785533, 1779777 etc.)
            + ``GNIS_PADDED``: Geographic Names Information System codes with seven-digit zero-padding (1779775, 1785533, 1779777 etc.)

        inplace : :obj:`bool` = False
            Determines whether to modify the original CensusDataFrame or create
            and return a new one.
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
    """
    An adapted :obj:`pandas.Series` object that stores additional information
    about the Census variable that corresponds to the data.
    """
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

    def clip_to_cb(self, inplace: bool = False) -> CensusGeoDataFrame | None:
        """
        Intersect the geographical boundaries of the CensusGeoDataFrame with the
        `Cartographic Boundaries <https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html>`_
        of the United States, which is a simplified representation of the United States
        boundary meant for thematic visualization.

        Parameters
        ==========
        inplace : :obj:`bool` = False
            Determines whether to modify the original CensusGeoDataFrame or 
            create and return a new one.
        """
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

    def remove_water(self, min_area: float = None, min_area_pct: float = None, top_n: int = None, top_pct: float = None, remove_internal: bool = False, inplace: bool = False) -> CensusGeoDataFrame | None:
        """
        Remove water geometries from within the geographical boundaries of the 
        CensusGeoDataFrame.

        Parameters
        ==========
        min_area : :obj:`float` = None
            Remove matching water geometries that are at least this square area (in 
            meters squared).
        min_area_pct : :obj:`float` = None
            Remove matching water geometries that are at least this percentage of the 
            total area of geometries in the CensusDataFrame.
        top_n : :obj:`int` = None
            Remove this many of the largest matching water geometries.
        top_pct : :obj:`float` = None
            Remove this percentage of the largest matching water geometries.
        remove_internal : :obj:`bool` = False
            Determines whether or not to remove water geometries that are strictly
            internal to the geographical boundaries of the CensusGeoDataFrame. For
            example, when this parameter is False, strictly internal lakes will not be
            removed.
        inplace : :obj:`bool` = False
            Determines whether to modify the original CensusGeoDataFrame or 
            create and return a new one.
        """
        geometry = union_all(self['geometry'].values)

        water_geom = _water_within_geometry(geometry=geometry, min_area=min_area, min_area_pct=min_area_pct, top_n=top_n, top_pct=top_pct, remove_internal=remove_internal)

        if inplace:
            if water_geom is not None:
                self['geometry'] = self.difference(water_geom)
        else:
            c = self.copy()
            c['geometry'] = c.difference(water_geom)
            return c

    def recompose_states(self, state_col: str = 'state', geometry_col: bool = 'geometry', position: str = Literal['below', 'outside'], preserve_area: bool = False, inplace: bool = False) -> CensusGeoDataFrame | None:
        """
        Shift the geographies of Alaska, Hawaii, and Puerto Rico so that they appear
        nearer to the contiguous United States.
        
        Inspired by Kyle Walker's `shift_geometry <https://github.com/walkerke/tigris/blob/master/R/shift_geometry.R>`_
        function in the ``tigris`` R package and the `geoAlbersUSA <https://d3js.org/d3-geo/conic#geoAlbersUsa>`_
        projection in the ``d3`` library.

        Parameters
        ==========
        state_col : :obj:`str` = 'state'
            The CensusGeoDataFrame's state column. Should be in padded FIPS format. See 
            the :meth:`.CensusDataFrame.recode` for automatically recoding your data to 
            this format.
        geometry_col : :obj:`str` = 'geometry'
            The CensusGeoDataFrame's geometry column.
        position : :obj:`str` = Literal['below', 'outside']
            Determines whether Alaska, Hawaii, and Puerto Rico should be position below
            our outside of the contiguous United States.
        preserve_area : :obj:`bool` = False
            Determines whether Alaska, Hawaii, and Puerto Rico should be rescaled,
            or whether their original area should be maintained.
        inplace : :obj:`bool` = False
            Determines whether to modify the original CensusGeoDataFrame or 
            create and return a new one.
        """
        return recompose_states(self, geometry_col=geometry_col, state_col=state_col, position=position, preserve_area=preserve_area, inplace=inplace)

    def compare_regions(self, comparison_col: str, geometry_col: bool = 'geometry', inplace: bool = False) -> CensusGeoDataFrame | None:
        """
        Transform a CensusGeoDataFrame's geometries so that two or more geographic regions can be
        visually compared.
        
        Rows are grouped by the specified ``comparison_col``, and geometries are
        repositioned and rescaled so that groups appear in a horizontal line.

        Parameters
        ==========
        comparison_col : :obj:`str`
            The column used to partition rows up into groups.
        geometry_col : :obj:`str` = 'geometry'
            The CensusGeoDataFrame's geometry column.
        inplace : :obj:`bool` = False
            Determines whether to modify the original CensusGeoDataFrame or 
            create and return a new one.
        """
        return compare_regions(self, comparison_col=comparison_col, geometry_col=geometry_col, inplace=inplace)