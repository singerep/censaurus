from pandas import DataFrame, Series
from geopandas import GeoDataFrame

# from .censusseries import CensusSeries
# from .dataset import Dataset
import censaurus.dataset


class CensusSeries(Series):
    _metadata = ["census_variable"]

    def __init__(self, data=None, index=None, **kwargs) -> None:
        name = kwargs.pop("name", None)
        get_census_variable = kwargs.pop("get_census_variable", None)

        super().__init__(data, index=index, name=name, **kwargs)

        self._census_variable = get_census_variable

    @property
    def _constructor(self):
        return CensusSeries

    @property
    def census_variable(self):
        return self._census_variable


class CensusDataFrame(DataFrame):
    def __init__(self, data, *args, **kwargs) -> None:
        super().__init__(data, *args, **kwargs)

        self._dataset = None

    @property
    def _constructor(self):
        return CensusDataFrame

    @property
    def _constructor_sliced(self):
        return CensusSeries

    @property
    def dataset(self):
        return self._dataset

    def _set_dataset(self, dataset):
        self._dataset = dataset

    def crosswalk(self, to):
        raise NotImplementedError