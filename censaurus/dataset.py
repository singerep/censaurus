import requests
from pandas import DataFrame, Series
import json
import typing
import os

dir_path = os.path.dirname(os.path.realpath(__file__))

# from censaurus import CensusDataFrame, Variable, VariableCollection, GeographyCollection
# from censaurus.censusdataframe import CensusDataFrame
# from censaurus.variable import Variable, VariableCollection
# from censaurus.geography import GeographyCollection
import censaurus.censusdataframe, censaurus.variable, censaurus.geography


class UnknownDataset(Exception):
    pass


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


class Dataset:
    def __init__(
        self,
        name: str,
        year: int,
        product_key: tuple,
        base_url: str,
        get_supported_geographies: bool = True,
        get_variables: bool = True,
    ) -> None:

        if product_key not in valid_dataset_keys:
            raise UnknownDataset(f"{product_key} is not a valid product key")

        self.name = name
        self.year = year
        self.product_key = product_key
        self.base_url = base_url

        self.supported_geographies = self._find_supported_geographies(get_supported_geographies)
        self.variables = self._find_variables(get_variables)

    def __repr__(self):
        return ' -> '.join([str(p) for p in self.product_key]) + f'\n  {len(self.supported_geographies)} supported geographies\n  {len(self.variables)} variables'

    @staticmethod
    def _make_product_key(**kwargs):
        raise NotImplementedError('all children of the Dataset class must implement this function')

    @staticmethod
    def _make_base_url(**kwargs):
        raise NotImplementedError('all children of the Dataset class must implement this function')

    @staticmethod
    def request(url):
        # TODO: add lots of checking of errors here
        resp = requests.get(url)
        status = resp.status_code
        data = resp.json()

        df = DataFrame(data)
        df.columns = df.iloc[0]
        df = df[1:]
        return df

    def _find_supported_geographies(self, get_supported_geographies: bool):
        if not get_supported_geographies:
            return None
        
        supported_geographies_url = f'{self.base_url}/geography.json'
        supported_geographies_json = requests.get(supported_geographies_url).json()['fips']
        supported_geographies = censaurus.geography.GeographyCollection(supported_geographies_json)
        return supported_geographies

    def _find_variables(self, get_variables: bool):
        if not get_variables:
            return None

        variables_url = f'{self.base_url}/variables.json'
        variables_json = requests.get(variables_url).json()['variables']
        variables = censaurus.variable.VariableCollection(variables_json)
        return variables

    def _build_request_url(self, geography_str: str, variable_str: str):
        url = self.base_url + f'?get={variable_str}&{geography_str}'
        return url

    def build_cdf(self, df):
        cdf = censaurus.censusdataframe.CensusDataFrame(data=df)
        cdf._set_dataset(self)
        return cdf

    def from_geography(
        self, 
        name: str = 'tract',
        filters: typing.Dict[str, str] = {},
        variables: \
            typing.Union[typing.List[str],\
            typing.List[censaurus.variable.Variable],\
            typing.List[typing.Union[str, censaurus.variable.Variable]],\
            censaurus.variable.VariableCollection] = [],
        groups: typing.List[str] = []
    ):
        geography, geography_str = self.supported_geographies._build_geography_string(
            name=name,
            filters=filters
        )
        variables, variable_str, rename_map = self.variables._build_variable_string(
            variables=variables,
            groups=groups
        )

        url = self._build_request_url(geography_str, variable_str)
        df = self.request(url)
        return self.build_cdf(df)


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
        base_url = self._make_base_url(year=year, product=product, extension=extension)

        super().__init__(
            name='ACS5',
            year=int(year),
            product_key=product_key,
            base_url=base_url,
            **kwargs
        )

    @staticmethod
    def _make_product_key(year, product, extension):
        key = (int(year), 'acs', product)
        if extension:
            key += (extension,)
        return key

    @staticmethod
    def _make_base_url(year, product, extension):
        url = f'https://api.census.gov/data/{year}/acs/{product}'
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


class ACS1_Supplemental(ACS):
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
    def _make_base_url(year, product, **kwargs):
        return f'https://api.census.gov/data/{year}/{product}'


class Decennial(Dataset):
    def __init__(
        self, 
        year: int = 2020,
        product: str = 'pl',
        **kwargs
    ) -> None:

        product_key = self._make_product_key(year=year, product=product)
        base_url = self._make_base_url(year=year, product=product)

        super().__init__(
            name='Decennial',
            year=int(year),
            product_key=product_key,
            base_url=base_url,
            **kwargs
        )

    @staticmethod
    def _make_product_key(year, product):
        key = (int(year), 'dec', product)
        return key

    @staticmethod
    def _make_base_url(year, product):
        url = f'https://api.census.gov/data/{year}/dec/{product}'
        return url