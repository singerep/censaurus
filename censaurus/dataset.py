from typing import Union, Dict, List, Tuple
from pandas import DataFrame, to_numeric
import json
import os
from geopandas import GeoDataFrame
from collections import defaultdict
import httpx

dir_path = os.path.dirname(os.path.realpath(__file__))

from censaurus.census_accessors import *
from censaurus.api import CensusClient
from censaurus.variable import Group, GroupCollection, Variable, VariableCollection
from censaurus.geography import GeographyCollection
from censaurus.tiger import AreaCollection, Area
from censaurus.constants import BAD_VALUES

def build_dataset_key(dataset_json):
    key = ()
    if 'c_vintage' in dataset_json:
        key += (int(dataset_json['c_vintage']),)
    if 'c_dataset' in dataset_json:
        key += tuple(dataset_json['c_dataset'])

    return key

datasets_resp = httpx.get('https://api.census.gov/data.json')
valid_dataset_keys = set(build_dataset_key(d) for d in datasets_resp.json()['dataset'])


class DatasetError(Exception):
    pass


class Dataset:
    """
    A base class to represent a Census dataset (product). This class **should not**
    be used if the dataset you desire already has its own :class:`.Dataset` subclass. 
    The following datasets **do** have :class:`.Dataset` subclasses, and so you should
    use those instead.

    * Decennial Census: :class:`.Decennial`

       + Decennial Census Redistricting Data: :class:`.DecennialPL`
       + Decennial Census Summary File 1: :class:`.DecennialSF1`
       + Decennial Census Summary File 2: :class:`.DecennialSF2`

    * American Community Survey Census: :class:`.ACS`

       + American Community Survey 1-Year Data: :class:`.ACS1`
       + American Community Survey 1-Year Supplemental Data: :class:`.ACSSupplemental`
       + American Community Survey 3-Year Data: :class:`.ACS3`
       + American Community Survey 5-Year Data: :class:`.ACS5`
       + American Community Survey Migration Flows: :class:`.ACSFlows`
       + American Community Survey Language Statistics: :class:`.ACSLanguage`

    * Public Use Microdata Sample :class:`.PUMS`

    * Current Population Survey :class:`.CPS`

    * Economic Census: :class:`.Economic`

       + Economic Census Key Statistics: :class:`.EconomicKeyStatistics`

    * Population Estimates: :class:`.Estimates`

    * Population Projections: :class:`.Projections`

    Parameters
    ==========
    product_key : :obj:`tuple` of :obj:`int` and/or :obj:`str`
        A unique key representing the dataset. Typically of the form 
        ``(<year>, <product>, <extension>)``. For example, ``(2021, "acs", "acs1")`` is 
        the key for the American Community Survey 1-Year Estimates published in 2021.
    url_extension : :obj:`str`
        A url path that accesses the content for this dataset. Appended to
        ``https://api.census.gov``. Typically, this looks like the elements of the 
        ``product_key`` joined by a '/'. For example, ``2021/acs/acs1`` is the extension
        for the American Community Survey 1-Year Estimates published in 2021.
    map_service : :obj:`str`
        The name of the TIGERWeb mapservice to use as the geographic basis for this
        dataset.
    """
    # TODO: do i need names or years?
    def __init__(
        self,
        product_key: Tuple[Union[int, str]],
        url_extension: str,
        map_service: str,
    ) -> None:

        if product_key not in valid_dataset_keys:
            raise DatasetError(f"{product_key} is not a valid product key")

        self.product_key = product_key
        self.url_extension = url_extension
        self.census_client = CensusClient(url_extension=url_extension)
        self.areas = AreaCollection(map_service=map_service)

        self._geographies = self._find_supported_geographies()
        self._variables = self._find_variables()

    def __repr__(self):
        dataset_str = f'{self.__class__.__name__} dataset object\n'
        dataset_str += f'  Product key: {self.product_key}\n'
        dataset_str += f'  {len(self.geographies)} supported geographies\n'
        dataset_str += f'  {len(self.variables)} variables'
        return dataset_str

    @property
    def geographies(self) -> GeographyCollection:
        '''
        An object representing the collection of supported geographies for this 
        dataset. Found by visiting
        ``https://api.census.gov/<url_extension>/geography.json``.
        '''
        return self._geographies

    @property
    def variables(self) -> VariableCollection:
        '''
        An object representing the collection of available variables for this 
        dataset. Found by visiting 
        ``https://api.census.gov/<url_extension>/variables.json``.
        '''
        return self._variables

    @property
    def groups(self) -> GroupCollection:
        '''
        An object representing the collection of groups of available variables for 
        this dataset.
        '''
        return self.variables.groups

    @staticmethod
    def _make_product_key(**kwargs):
        raise NotImplementedError('all children of the Dataset class must implement this function')

    @staticmethod
    def _make_url_extension(**kwargs):
        raise NotImplementedError('all children of the Dataset class must implement this function')

    def _find_supported_geographies(self):
        supported_geographies_response = self.census_client.get_sync('/geography.json')
        supported_geographies_json = supported_geographies_response.json()['fips']
        supported_geographies = GeographyCollection(supported_geographies_json)
        return supported_geographies

    def _find_variables(self):
        variables_json = self.census_client.get_sync('/variables.json').json()['variables']
        variables = VariableCollection(variables_json)
        return variables

    def _build_request_url(self, geography_str: str, variable_str: str):
        url = self.url_extension + f'?get={variable_str}&{geography_str}'
        return url

    def _get_cdf(self, within: Union[Area, List[Area]], target: str, target_layer_name: Union[str, List[str]], variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]], groups: Union[List[str], List[Group]], return_geometry: bool, area_threshold: float, extra_census_params: Dict[str, str] = None) -> Union[GeoDataFrame, DataFrame]:
        variables, variable_params_list, rename_map = self.variables._build_variable_params(variables=variables, groups=groups)
        geography, geography_params_list, features_within = self.geographies._build_geography_params(areas=self.areas, within=within, target=target, target_layer_name=target_layer_name, return_geometry=return_geometry, area_threshold=area_threshold)

        params_list = []
        for geography_params in geography_params_list:
            for variable_params in variable_params_list:
                params = {}
                params.update(geography_params)
                params.update(variable_params)
                if extra_census_params:
                    params.update(extra_census_params)
                params_list.append(params)
        url_list = ['']*len(params_list)
        url_params_list = zip(url_list, params_list)
        responses = self.census_client.get_many_sync(url_params_list=url_params_list)

        if all(resp.status_code == 200 for resp in responses):
            try:
                dfs : List[DataFrame] = []
                for resp in responses:
                    data = resp.json()
                    df = DataFrame(data)
                    df.columns = df.iloc[0]
                    df = df[1:]
                    df = df.reset_index(drop=True)
                    df = df.rename_axis(None, axis=1)
                    dfs.append(df)

                geo_id_map = defaultdict(dict)
                for df in dfs:
                    df_records = df.to_dict(orient='records')
                    for geo_id, record in zip(df['GEO_ID'], df_records):
                        geo_id_map[geo_id].update(record)
                df = DataFrame.from_records(list(geo_id_map.values()))
                
            except json.decoder.JSONDecodeError:
                raise DatasetError(f'There was a problem decoding the result of your Census API call.')
        elif any(resp.status_code == 204 for resp in responses):
            # TODO: THIS IS NOT NECESSARILY DESIRED BEHAVIOR -- POSSIBLY ONLY SOME REQUESTS HAVE NO DATA
            return DataFrame()   
        else:
            # raise DatasetError(f'The Census API had an error ({resp.status_code}) and returned the following message: {resp.text}')
            # TODO: need to have a list of unique errors here
            raise DatasetError()

        if features_within is not None:
            df['GEOID'] = df['GEO_ID'].apply(lambda g : g.split('US')[1] if g != '0100000US' else g)
            if return_geometry is True:
                df = GeoDataFrame(df.merge(features_within[['GEOID', 'geometry']], on='GEOID', how='inner')).drop(labels='GEOID', axis=1)
                df.set_crs(crs='4236')
            else:
                df = df.merge(features_within[['GEOID']], on='GEOID', how='inner').drop(labels='GEOID', axis=1)

        if rename_map != {}:
            reverse_rename_map = {v: k for k, v in rename_map.items()}
            df = df.rename(columns=rename_map)

        for val in BAD_VALUES:
            df = df.replace(val, None)

        df.census.geography = geography
        df.census.variables = variables

        for col_name in df.columns:
            if rename_map != {}:
                var_name = reverse_rename_map.get(col_name, col_name)
            else:
                var_name = col_name
            variable = self.variables.get(variable=var_name)
            if variable is not None:
                if variable.type == int:
                    df[col_name] = df[col_name].apply(to_numeric, errors='coerce').fillna(df[col_name])

        for col_name in df.columns:
            if rename_map != {}:
                var_name = reverse_rename_map.get(col_name, col_name)
            else:
                var_name = col_name
            variable = self.variables.get(variable=var_name)
            if variable is not None:
                df[col_name].census.variable = variable

        return df

    def us(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None) -> Union[DataFrame, GeoDataFrame]:
        """
        Get Census data for the entire United States.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='us', target_layer_name=None, variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def regions(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for regions of United States. See 
        `here <https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf>`_
        for an overview of Census regions.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='region', target_layer_name='Census Regions', variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def divisions(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for divisions of United States. See 
        `here <https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf>`_
        for an overview of Census divisions.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='division', target_layer_name='Census Divisions', variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def states(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for states.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='state', target_layer_name='States', variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def counties(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for counties.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='county', target_layer_name='Counties', variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def county_subdivisions(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for county subdivisions.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='county subdivision', target_layer_name='County Subdivisions', variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def tracts(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for Census tracts.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='tract', target_layer_name='Census Tracts', variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def block_groups(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for Census block groups.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='block group', target_layer_name='Census Block Groups', variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def blocks(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for Census blocks.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='block', target_layer_name='Census Blocks', variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def places(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for Census places (both Incorporated Places and Census 
        Designated Places).

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='place', target_layer_name=['Census Designated Places', 'Incorporated Places'], variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def MSAs(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for Metropolitan and Micropolitan Statistical Areas.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='metropolitan statistical area/micropolitan statistical area', target_layer_name=['Metropolitan Statistical Areas', 'Micropolitan Statistical Areas'], variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def CSAs(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for Combined Statistical Areas.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='combined statistical area', target_layer_name='Combined Statistical Areas', variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def congressional_districts(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for Congressional Districts.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='congressional district', target_layer_name='Congressional Districts', variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def voting_districts(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for voting districts.

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='voting district', target_layer_name='Voting Districts', variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def ZCTAs(self, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for Zib Code Tabulation Areas (note that these are **not**
        the exact same as zip codes).

        Parameters
        ==========
        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target='zip code tabulation area', target_layer_name='Zip Code Tabulation Areas', variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)

    def other_geography(self, geography: str, geography_layer: str = None, within: Union[Area, List[Area]] = None, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, area_threshold: float = 0.01, extra_census_params: Dict[str, str] = None):
        """
        Get Census data for any other geographic level supported by the Census.

        Parameters
        ==========
        geography : :obj:`str`
            The geographic level to get data from. To see the available geographies,
            see :attr:`.Dataset.geographies`.

        geography_layer : :obj:`str` = None
            The TIGERWeb MapService geographic layer to use for area geometries. Only
            necessary if 1) you want to return geometries or 2) you are not using a
            default geographic Census hierarchy.

        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. If ``within`` is ``None``, then
            all areas with the entire (cartographic) boundary of the United States will
            be included. Note that what it means for an area to be "within" depends on
            the ``area_threshold`` parameter.

        variables : :obj:`list` of :obj:`str` or :obj:`list` of :class:`.Variable` or :obj:`list` of mixed :obj:`str` and :class:`.Variable` or :class:`.VariableCollection` or dict of :obj:`str`: :obj:`str` = []
            The Census variables to get. If ``variables`` is a dictionary, then 
            variables will also be renamed. For example, setting 
            ``variables={"B01001_001E": "total_pop"}`` would rename the ``B01001_001E``
            variable to ``total_pop`` in the resulting :class:`pandas.DataFrame`. Note
            that NAME and GEO_ID will be added to this list if you do not include them.

        groups : :obj:`list` of :obj:`str` or :obj:`list` of :class:`Group` = []
            The Census groups to get. All variables within each group in ``groups``
            will be requested.

        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each geographic area should be 
            returned.

        area_threshold : :obj:`float` = 0.01
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).

        extra_census_params : dict of :obj:`str`: :obj:`str` = {}
            Extra query parameters to pass to the Census when requesting data.
        """
        return self._get_cdf(within=within, target=geography, target_layer_name=geography_layer, variables=variables, groups=groups, return_geometry=return_geometry, area_threshold=area_threshold, extra_census_params=extra_census_params)


# --- SPECIFIC DATASETS ---
class ACS(Dataset):
    """
    Data from the American Community Survey. Generally, there is no need to instantiate
    an object of this class: you should use one of the subclasses instead. The available
    subclasses are:

       + American Community Survey 1-Year Data: :class:`.ACS1`
       + American Community Survey 1-Year Supplemental Data: :class:`.ACSSupplemental`
       + American Community Survey 3-Year Data: :class:`.ACS3`
       + American Community Survey 5-Year Data: :class:`.ACS5`
       + American Community Survey Migration Flows: :class:`.ACSFlows`
       + American Community Survey Language Statistics: :class:`.ACSLanguage`

    Parameters
    ==========
    year : :obj:`int` = 2021
        The year to get data from. Selected ACS data is available from 2005 to 
        2021.
    product : :obj:`str` = 'acs5'
        The specific American Community Survey product to get data from. Available
        products are:

           + American Community Survey 1-Year Data: ``acs1``
           + American Community Survey 1-Year Supplemental Data: ``acsse``
           + American Community Survey 3-Year Data: ``acs3``
           + American Community Survey 5-Year Data: ``acs5``
           + American Community Survey Migration Flows: ``flows``
           + American Community Survey Language Statistics: ``language``
        
    extension : :obj:`str` = None
        The specific American Community Survey product extension to get data from.
        Example extension include:

           + Subject Tables: ``subject``
           + Data Profiles: ``profile``
           + Comparison Profile: ``cprofile``
           + Selected Population Profiles: ``spp``

        However, the available extensions depend on the product used. The extensions
        above are for the American Community Survey 1-Year Estimates.
    """
    def __init__(
        self, 
        year: int = 2021,
        product: str = "acs5",
        extension: str = None,
        **kwargs
    ) -> None:

        product_key = self._make_product_key(year=year, product=product, extension=extension)
        url_extension = self._make_url_extension(year=year, product=product, extension=extension)

        super().__init__(
            product_key=product_key,
            url_extension=url_extension,
            map_service=f'tigerWMS_ACS{year}',
            **kwargs
        )

    @staticmethod
    def _make_product_key(year, product, extension):
        key = (int(year), 'acs', product)
        if extension:
            key += (extension,)
        return key

    @staticmethod
    def _make_url_extension(year, product, extension):
        url = f'{year}/acs/{product}'
        if extension:
            url += f'/{extension}'
        return url


class ACS1(ACS):
    """
    Data from the American Community Survey 1-Year Estimates.

    Parameters
    ==========
    year : :obj:`int` = 2021
        The year to get data from. Selected ACS1 data is available from 2005 to 
        2021.
        
    extension : :obj:`str` = None
        The specific ACS1 product extension to get data from. Defaults to ``None``,
        which maps to Detailed Tables. Other available extensions are:

           + Subject Tables: ``subject``
           + Data Profiles: ``profile``
           + Comparison Profile: ``cprofile``
           + Selected Population Profiles: ``spp``
    """
    def __init__(self, year: int = 2021, extension: str = None, **kwargs) -> None:
        super().__init__(
            year=year,
            product='acs1',
            extension=extension, 
            **kwargs
        )


class ACS3(ACS):
    """
    Data from the American Community Survey 3-Year Estimates.

    Parameters
    ==========
    year : :obj:`int` = 2013
        The year to get data from. Selected ACS3 data is available from 2007 to 
        2013.
        
    extension : :obj:`str` = None
        The specific ACS1 product extension to get data from. Defaults to ``None``,
        which maps to Detailed Tables. Other available extensions are:

           + Subject Tables: ``subject``
           + Data Profiles: ``profile``
           + Comparison Profile: ``cprofile``
           + Selected Population Profiles: ``spp``
    """
    def __init__(self, year: int = 2013, extension: str = None, **kwargs) -> None:
        super().__init__(
            year=year,
            product='acs3',
            extension=extension, 
            **kwargs
        )


class ACS5(ACS):
    """
    Data from the American Community Survey 5-Year Estimates.

    Parameters
    ==========
    year : :obj:`int` = 2021
        The year to get data from. Selected ACS5 data is available from 2009 to 
        2021.
        
    extension : :obj:`str` = None
        The specific ACS5 product extension to get data from. Defaults to ``None``,
        which maps to Detailed Tables. Other available extensions are:

           + Subject Tables: ``subject``
           + Data Profiles: ``profile``
           + Comparison Profile: ``cprofile``
           + Selected Population Data Profiles: ``sptprofile``
           + American Indian and Alaska Native Detailed Tables: ``aian``
           + American Indian and Alaska Native Data Profiles: ``aianprofile``
    """
    def __init__(self, year: int = 2021, extension: str = None, **kwargs) -> None:
        super().__init__(
            year=year,
            product='acs5',
            extension=extension, 
            **kwargs
        )


class ACSSupplemental(ACS):
    """
    Data from the American Community Survey 1-Year Supplemental Data.

    Parameters
    ==========
    year : :obj:`int` = 2021
        The year to get data from. Selected ACS Supplement data is available from 2014 to 
        2021.
    """
    def __init__(self, year: int = 2021, **kwargs) -> None:
        super().__init__(
            year=year,
            product='acsse',
            **kwargs
        )


class ACSFlows(ACS):
    """
    Data from the American Community Survey Migration Flows.

    Parameters
    ==========
    year : :obj:`int` = 2020
        The year to get data from. ACS Flows data is available from 2010 to 
        2020.
    """
    def __init__(self, year: int = 2020, **kwargs) -> None:
        super().__init__(
            year=year,
            product='flows',
            **kwargs
        )


class ACSLanguage(ACS):
    """
    Data from the American Community Survey Language Statistics (2013).
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(
            year=2013,
            product='language',
            **kwargs
        )

    @staticmethod
    def _make_product_key(year, product, **kwargs):
        return (int(year), product)

    @staticmethod
    def _make_url_extension(year, product, **kwargs):
        return f'{year}/{product}'


class PUMS(ACS):
    """
    Data from the Public Use Microdata Sample.

    Parameters
    ==========
    year : :obj:`int` = 2021
        The year to get data from. Selected PUMS data is available from 2005 to 
        2021.

    duration : :obj:`int` = 5
        THe duration of the sample. Indicates whether to get 5 or 1 year estimates.
        
    puerto_rico : :obj:`bool` = False
        Determines whether or not to get data from the PUMS Puerto Rico survey.
    """
    def __init__(self, year: int = 2021, duration: Union[str, int] = 5, puerto_rico: bool = False, **kwargs) -> None:
        if puerto_rico is False:
            extension = 'pumspr'
        else:
            extension = 'pums'
        super().__init__(
            year=year, 
            product=f'acs{duration}', 
            extension=extension,
            **kwargs
        )


class CPS(Dataset):
    """
    Data from the Current Population Survey.

    Parameters
    ==========
    year : :obj:`int` = 2023
        The year to get data from. Selected CPS data is available from 1989 to 2023.
    month : :obj:`str` = 'jan'
        The month to get data from. Should be three letters long (jan, feb, mar, etc.).
        Not all months are available every year and for every product.
    product : :obj:`str` = 'basic'
        The specific Current Population Survey product to get data from. Example 
        products include:

           + Arts Benchmarking Supplement: ``arts``
           + Basic Monthly: ``basic``
           + Civic Engagement Supplement: ``civic``
           + Computer and Internet Use Supplement: ``internet``
           + Contingent Worker Supplement: ``contworker``
           + Disability Supplement: ``disability``

        However, the available products depend on the year and month requested.
    """
    def __init__(self, year: int = 2023, month: int = 'jan', product: str = 'basic', **kwargs) -> None:

        product_key = self._make_product_key(year=year, month=month, product=product)
        url_extension = self._make_url_extension(year=year, month=month, product=product)

        super().__init__(
            product_key=product_key,
            url_extension=url_extension,
            map_service = 'tigerWMS_Current',
            **kwargs
        )

    @staticmethod
    def _make_product_key(year, month, product):
        return (int(year), 'cps', product, month)

    @staticmethod
    def _make_url_extension(year, month, product):
        return f'{year}/cps/{product}/{month}'


class Decennial(Dataset):
    """
    Data from the Decennial Census. This class **should not** be used if the dataset you
    desire already had its own :class:`.Dataset` subclass that inherits from 
    :class:`.Decennial`. The available subclasses are:

       + Decennial Redistricting Data: :class:`.DecennialPL`
       + Decennial Summary File 1: :class:`.DecennialSF1`
       + Decennial Summary File 2: :class:`.DecennialSF2`

    Parameters
    ==========
    year : :obj:`int` = 2020
        The year to get data from. Selected Decennial Census data is available in 2000,
        2010, and 2020.
    product : :obj:`str` = 'pl'
        The specific Decennial Census product to get data from. Example products
        include:

           + Demographic and Housing Characteristics File: ``dhc``
           + Demographic Profile: ``dp``
           + Redistricting Data: ``pl``
           + Decennial Post-Enumeration Survey: ``pes``
           + Island Areas Demographic and Housing Characteristics File: ``dhcas``
           + Island Areas Demographic Profile: ``dpas``

        However, the available products depend on the year requested. The products
        above are for 2020 Decennial Census. More products are available for the 2010
        and 2000 Decennial Censuses.
    """
    def __init__(
        self, 
        year: int = 2020,
        product: str = 'pl',
        **kwargs
    ) -> None:

        product_key = self._make_product_key(year=year, product=product)
        url_extension = self._make_url_extension(year=year, product=product)

        super().__init__(
            product_key=product_key,
            url_extension=url_extension,
            map_service=f'tigerWMS_Census{year}',
            **kwargs
        )

    @staticmethod
    def _make_product_key(year, product):
        key = (int(year), 'dec', product)
        return key

    @staticmethod
    def _make_url_extension(year, product):
        url = f'{year}/dec/{product}'
        return url


class DecennialPL(Decennial):
    """
    Data from the Decennial Census Redistricting Data.

    Parameters
    ==========
    year : :obj:`int` = 2020
        The year to get data from. Redistricting data is available in 2000, 2010, or
        2020.
    """
    def __init__(self, year: int = 2020, **kwargs) -> None:
        super().__init__(
            year=year,
            product='pl',
            **kwargs
        )


class DecennialSF1(Decennial):
    """
    Data from the Decennial Census Summary File 1.

    Parameters
    ==========
    year : :obj:`int` = 2010
        The year to get data from. SF1 data is available in 2000 or 2010.
    """
    def __init__(self, year: int = 2010, **kwargs) -> None:
        super().__init__(
            year=year,
            product='sf1',
            **kwargs
        )

class DecennialSF2(Decennial):
    """
    Data from the Decennial Census Summary File 2.

    Parameters
    ==========
    year : :obj:`int` = 2010
        The year to get data from. SF2 data is available in 2000 or 2010.
    """
    def __init__(self, year: int = 2010, **kwargs) -> None:
        super().__init__(
            year=year,
            product='sf2',
            **kwargs
        )


class Economic(Dataset):
    """
    Data from the Economic Census. This class **should only** be used if you desire
    something **other** than the Census's Economy-Wide Key Statistics. If you do in
    fact want key statistics, you should use the :class:`.EconomicKeyStatistics` 
    subclass instead.

    Parameters
    ==========
    year : :obj:`int` = 2017
        The year to get data from. Selected Economic Census data is available in 2002,
        2007, 2012, and 2017.
    product : :obj:`str` = 'pl'
        The specific Economic Census product to get data from. Example products
        include:

           + Economy-Wide Key Statistics: ``ecnbasic`` in 2017, ``ewks`` otherwise
           + Economic Census of Island Areas: ``ecn/islandareas/napcs``
           + Brokering and Dealing Products Income for the U.S.: ``ecnbranddeal``
    """
    def __init__(
        self, 
        year: int = 2017,
        product: str = 'ecnbasic',
        **kwargs
    ) -> None:

        product_key = self._make_product_key(year=year, product=product)
        url_extension = self._make_url_extension(year=year, product=product)

        super().__init__(
            product_key=product_key,
            url_extension=url_extension,
            map_service=f'tigerWMS_Current',
            **kwargs
        )

    @staticmethod
    def _make_product_key(year, product):
        key = (int(year),) + tuple(product.split('/'))
        return key

    @staticmethod
    def _make_url_extension(year, product):
        url = f'{year}/{product}'
        return url


class EconomicKeyStatistics(Economic):
    """
    Data from the Economic Census Economy-Wide Key Statistics.

    Parameters
    ==========
    year : :obj:`int` = 2017
        The year to get data from. EWKS data is available in 2002, 2007, 2012, or
        2017.
    """
    def __init__(self, year: int = 2017, **kwargs) -> None:
        if year == 2017:
            product = 'ecnbasic'
        else:
            product = 'ewks'
        super().__init__(
            year=year, 
            product=product,
            **kwargs
        )


class Estimates(Dataset):
    """
    Data from the Census Population Estimates.

    Parameters
    ==========
    year : :obj:`int` = 2021
        The year to get data from. Selected Population Estimates data is available from
        2013 to 2021.
    monthly : :obj:`bool` = False
        Determines whether or not to get yearly or monthly population estimates.
    """
    def __init__(self, year: int = 2021, monthly: bool = False, **kwargs) -> None:

        product_key = self._make_product_key(year=year, monthly=monthly)
        url_extension = self._make_url_extension(year=year, monthly=monthly)

        super().__init__(
            product_key=product_key,
            url_extension=url_extension,
            map_service = 'tigerWMS_Current',
            **kwargs
        )

    @staticmethod
    def _make_product_key(year, monthly: bool = False):
        key = (int(year), 'pep')
        if monthly:
            key = key + ('natmonthly',)
        else:
            key = key + ('population',)
        return key

    @staticmethod
    def _make_url_extension(year, monthly: bool = False):
        url = f'{year}/pep/'
        if monthly:
            url += 'natmonthly'
        else:
            url += 'population'
        return url


class Projections(Dataset):
    """
    Data from the Census Population Projections.

    Parameters
    ==========
    year : :obj:`int` = 2017
        The year to get data from. Selected Population Projections data is available 
        in 2012, 2014, and 2017.
    extension : :obj:`str` = 'pop'
        The specific Census Population Projections extension to get data from. Example 
        extensions include:

           + Projected Population by Demographic Characteristics: ``pop``
           + Projected Population by Demographic Characteristics: ``agegroups``
           + Projected Population by Demographic Characteristics (Single Year of Age) and Nativity: ``nat``
           + Projected Births by Demographic Characteristics: ``births``
           + Projected Deaths by Demographic Characteristics: ``deaths``
           + Projected Net International Migration by Demographic Characteristics: ``nim``

        However, the available extensions depend on the year requested. The extensions
        above are for 2017 Population Projections.
    """
    def __init__(self, year: int = 2017, extension: str = 'pop', **kwargs) -> None:

        product_key = self._make_product_key(year=year, extension=extension)
        url_extension = self._make_url_extension(year=year, extension=extension)

        super().__init__(
            product_key=product_key,
            url_extension=url_extension,
            map_service = 'tigerWMS_Current',
            **kwargs
        )

    @staticmethod
    def _make_product_key(year, extension):
        return (int(year), 'popproj', extension)

    @staticmethod
    def _make_url_extension(year, extension):
        return f'{year}/popproj/{extension}'