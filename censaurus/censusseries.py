from pandas import Series

from .censusvariable import CensusVariable

class CensusSeries(Series):
    _metadata = ["census_variable"]

    def __init__(self, data=None, index=None, **kwargs) -> None:
        name = kwargs.pop("name", None)
        get_census_variable = kwargs.pop("get_census_variable", None)

        super().__init__(data, index=index, name=name, **kwargs)

        # self._census_variable = CensusVariable(name=name, variables=[])
        self._census_variable = get_census_variable

    @property
    def _constructor(self):
        return CensusSeries

    @property
    def census_variable(self):
        return self._census_variable