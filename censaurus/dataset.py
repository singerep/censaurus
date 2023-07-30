from typing import Union, Dict, List, Callable, Tuple
from pandas import DataFrame, Series, to_numeric, concat, merge
import json
import typing
import os
import asyncio
import matplotlib.pyplot as plt
from geopandas import GeoDataFrame
from shapely import union_all
from collections import defaultdict

dir_path = os.path.dirname(os.path.realpath(__file__))

import censaurus.census_accessors
from censaurus.api import CensusClient, TIGERClient
from censaurus.variable import Group, GroupCollection, Variable, VariableCollection
from censaurus.geography import Geography, GeographyCollection, InvalidGeographyHierarchy
from censaurus.tiger import AreaCollection, Area, US_CARTOGRAPHIC
from censaurus.constants import LAYER_NAME_MAP

# TODO: change this to a request later, but too slow for testing
with open(dir_path + '/data/datasets.json') as file:
    datasets_json = json.load(file)['dataset']

def build_dataset_key(dataset_json):
    key = ()
    if 'c_vintage' in dataset_json:
        key += (int(dataset_json['c_vintage']),)
    if 'c_dataset' in dataset_json:
        key += tuple(dataset_json['c_dataset'])

    return key

valid_dataset_keys = set(build_dataset_key(d) for d in datasets_json)


class DatasetError(Exception):
    pass


class Dataset:
    """
    A base class to represent a Census dataset (product). This class **should not**
    be used if the dataset you desire already has its own :class:`.Dataset` subclass. 
    The following datasets **do** have :class:`.Dataset` subclasses, and so those should 
    be used instead.

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

    * Economic Census: :class:`.Economic`

       + Economic Census Key Statistics: :class:`.EconomicKeyStatistics`

    * Population Estimates: :class:`.Estimates`

    * Population Projections: :class:`.Projections`

    Parameters
    ==========
    name: str
        The name of the dataset.
    year: int
        The year of the dataset.
    product_key: Tuple[Union[int, str]]
        A unique key representing the dataset. Typically of the form 
        (<year>, <product>, <extension>). For example, (2021, 'acs', 'acs1') is the key
        for the American Community Survey 1-Year Estimates published in 2021.
    url_extension: str
        A url path that accesses the content for this dataset. Appended to
        https://api.census.gov. Typically, this looks like the elements of the product
        key joined by a '/'.
    """
    # TODO: do i need names or years?
    def __init__(
        self,
        name: str,
        year: Union[int, str],
        product_key: Tuple[Union[int, str]],
        url_extension: str,
        map_service: str,
        get_supported_geographies: bool = True,
        get_variables: bool = True,
    ) -> None:

        if product_key not in valid_dataset_keys:
            raise DatasetError(f"{product_key} is not a valid product key")

        self.name = name
        self.year = year
        self.product_key = product_key
        self.url_extension = url_extension
        self.census_client = CensusClient(url_extension=url_extension)
        self.areas = AreaCollection(map_service=map_service)

        self._geographies = self._find_supported_geographies(get_supported_geographies)
        self._variables = self._find_variables(get_variables)

    def __repr__(self):
        return ' -> '.join([str(p) for p in self.product_key]) + f'\n  {len(self.geographies)} supported geographies\n  {len(self.variables)} variables'

    @property
    def geographies(self):
        return self._geographies

    @property
    def variables(self):
        return self._variables

    @property
    def groups(self):
        return self.variables.groups

    @staticmethod
    def _make_product_key(**kwargs):
        raise NotImplementedError('all children of the Dataset class must implement this function')

    @staticmethod
    def _make_url_extension(**kwargs):
        raise NotImplementedError('all children of the Dataset class must implement this function')

    def _find_supported_geographies(self, get_supported_geographies: bool):
        if not get_supported_geographies:
            return None
        
        supported_geographies_response = self.census_client.get_sync('/geography.json')
        supported_geographies_json = supported_geographies_response.json()['fips']
        supported_geographies = GeographyCollection(supported_geographies_json)
        return supported_geographies

    def _find_variables(self, get_variables: bool):
        if not get_variables:
            return None

        variables_json = self.census_client.get_sync('/variables.json').json()['variables']
        variables = VariableCollection(variables_json)
        return variables

    def _build_request_url(self, geography_str: str, variable_str: str):
        url = self.url_extension + f'?get={variable_str}&{geography_str}'
        return url

    def _get_cdf(self, within: Union[Area, List[Area]], target: str, target_layer_name: str, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]], groups: Union[List[str], List[Group]], return_geometry: bool, extra_census_params: Dict[str, str] = None) -> Union[GeoDataFrame, DataFrame]:
        variables, variable_params_list, rename_map = self.variables._build_variable_params(variables=variables, groups=groups)
        geography, geography_params_list, features_within = self.geographies._build_geography_params(areas=self.areas, within=within, target=target, target_layer_name=target_layer_name, return_geometry=return_geometry)

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
                raise DatasetError(f'There was a problem decoding the result of your API call.')
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
            else:
                df = df.merge(features_within[['GEOID']], on='GEOID', how='inner').drop(labels='GEOID', axis=1)

        # TODO: should only do this if it needs to? possibly can infer -- should use predicateType in Variable
        df = df.apply(to_numeric, errors='coerce').fillna(df)

        df.census.geography = geography
        df.census.variables = variables

        for col in df.columns:
            variable = self.variables.get(variable=col)
            if variable is not None:
                df[col].census.variable = variable

        if rename_map != {}:
            df = df.rename(columns=rename_map)
        
        return df

    def us(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='us', target_layer_name=None, variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def regions(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='region', target_layer_name='Census Regions', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def divisions(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='division', target_layer_name='Census Divisions', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def states(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='state', target_layer_name='States', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def counties(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='county', target_layer_name='Counties', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def county_subdivisions(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='county subdivision', target_layer_name='County Subdivisions', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def tracts(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='tract', target_layer_name='Census Tracts', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def block_groups(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='block group', target_layer_name='Census Block Groups', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def blocks(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='block', target_layer_name='Census Blocks', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def places(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='place', target_layer_name=['Census Designated Places', 'Incorporated Places'], variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def MSAs(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='county', target_layer_name='Counties', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def CSAs(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='combined statistical area', target_layer_name='Combined Statistical Areas', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def congressional_districts(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='congressional district', target_layer_name='Congressional Districts', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def voting_districts(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='voting district', target_layer_name='Voting Districts', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def ZCTAs(self, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target='zip code tabulation area', target_layer_name='Zip Code Tabulation Areas', variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)

    def other_geography(self, geography: str, geography_layer: str = None, within: Union[Area, List[Area]] = US_CARTOGRAPHIC, variables: Union[List[str], List[Variable], List[Union[str, Variable]], VariableCollection, Dict[str, str]] = [], groups: Union[List[str], List[Group], List[Union[str, Group]], GroupCollection] = [], return_geometry: bool = False, extra_census_params: Dict[str, str] = None):
        return self._get_cdf(within=within, target=geography, target_layer_name=geography_layer, variables=variables, groups=groups, return_geometry=return_geometry, extra_census_params=extra_census_params)


# --- SPECIFIC DATASETS ---
class ACS(Dataset):
    def __init__(
        self, 
        year: int = 2021,
        product: str = 'acs5',
        extension: str = None,
        **kwargs
    ) -> None:

        product_key = self._make_product_key(year=year, product=product, extension=extension)
        url_extension = self._make_url_extension(year=year, product=product, extension=extension)

        super().__init__(
            name='ACS5',
            year=int(year),
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
    def __init__(self, year: int = 2021, extension: str = None, **kwargs) -> None:
        super().__init__(
            year=year,
            product='acs1',
            extension=extension, 
            **kwargs
        )


class ACS3(ACS):
    def __init__(self, year: int = 2013, extension: str = None, **kwargs) -> None:
        super().__init__(
            year=year,
            product='acs3',
            extension=extension, 
            **kwargs
        )


class ACS5(ACS):
    def __init__(self, year: int = 2021, extension: str = None, **kwargs) -> None:
        super().__init__(
            year=year,
            product='acs5',
            extension=extension, 
            **kwargs
        )


class ACSSupplemental(ACS):
    def __init__(self, year: int = 2021, **kwargs) -> None:
        super().__init__(
            year=year,
            product='acsse',
            **kwargs
        )


class ACSFlows(ACS):
    def __init__(self, year: int = 2020, **kwargs) -> None:
        super().__init__(
            year=year,
            product='flows',
            **kwargs
        )


class ACSLanguage(ACS):
    def __init__(self, year: int = 2013, **kwargs) -> None:
        super().__init__(
            year=year,
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
    def __init__(self, year: int = 2021, duration: Union[str, int] = 5, **kwargs) -> None:
        product = f'acs{duration}'
        super().__init__(
            year=year, 
            product=product, 
            extension='pums',
            **kwargs
        )


class Decennial(Dataset):
    def __init__(
        self, 
        year: int = 2020,
        product: str = 'pl',
        **kwargs
    ) -> None:

        product_key = self._make_product_key(year=year, product=product)
        url_extension = self._make_url_extension(year=year, product=product)

        super().__init__(
            name='Decennial',
            year=int(year),
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
    def __init__(self, year: int = 2020, **kwargs) -> None:
        super().__init__(
            year=year,
            product='pl',
            **kwargs
        )


class DecennialSF1(Decennial):
    def __init__(self, year: int = 2010, **kwargs) -> None:
        super().__init__(
            year=year,
            product='sf1',
            **kwargs
        )

class DecennialSF2(Decennial):
    def __init__(self, year: int = 2010, **kwargs) -> None:
        super().__init__(
            year=year,
            product='sf2',
            **kwargs
        )


class Economic(Dataset):
    def __init__(
        self, 
        year: int = 2017,
        product: str = 'ecnbasic',
        **kwargs
    ) -> None:

        product_key = self._make_product_key(year=year, product=product)
        url_extension = self._make_url_extension(year=year, product=product)

        super().__init__(
            name='Economic Census',
            year=int(year),
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
    def __init__(self, year: int = 2021, monthly: bool = False, **kwargs) -> None:

        product_key = self._make_product_key(year=year, monthly=monthly)
        url_extension = self._make_url_extension(year=year, monthly=monthly)

        super().__init__(
            name='Estimates',
            year=year,
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
    def __init__(self, year: int = 2017, extension: str = 'pop', **kwargs) -> None:

        product_key = self._make_product_key(year=year, extension=extension)
        url_extension = self._make_url_extension(year=year, extension=extension)

        super().__init__(
            name='Projections',
            year=year,
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