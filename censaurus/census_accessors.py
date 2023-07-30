from pandas.api.extensions import register_dataframe_accessor, register_series_accessor

from censaurus.geography import Geography
from censaurus.variable import Variable, VariableCollection


@register_dataframe_accessor('census')
class DataFrameCensusAccessor:
    def __init__(self, pandas_obj) -> None:
        self._obj = pandas_obj
        self._geography : Geography = None
        self._variables : VariableCollection = None
    
    @property
    def geography(self):
        return self._geography

    @geography.setter
    def geography(self, geography: Geography):
        self._geography = geography

    @property
    def variables(self):
        return self._variables

    @variables.setter
    def variables(self, variables: VariableCollection):
        self._variables = variables


@register_series_accessor('census')
class SeriesCensusAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self._variable : Variable = None

    @property
    def variable(self):
        return self._variable
    
    @variable.setter
    def variable(self, variable: Variable):
        self._variable = variable