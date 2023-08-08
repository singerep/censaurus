from typing import Dict, List, Tuple, Iterable, Union
from pandas import DataFrame
from collections import defaultdict
import re

from censaurus.variable import VariableCollection, RegroupedVariable
from censaurus.rename import AGE_REGEX


class Regrouper:
    def __init__(self, groupings: Dict[str, Iterable[str]] = {}) -> None:
        self.groupings = groupings

    def regroup(self, data: DataFrame):
        variables = data.census.variables

        variable_map = {}
        column_map = {}
        grouped_cols = defaultdict(list)
        grouped_variables = defaultdict(list)
        # variables_to_drop = set()
        
        for c in data.columns:
            variable = data[c].census.variable
            variable_map[c] = variable
            if variable is not None:
                column_map[variable.name] = c
                list_path = list(variable.path)
                for group, group_elements in self.groupings.items():
                    for i, element in enumerate(list_path):
                        for group_element in group_elements:
                            if element == group_element:
                                grouped_path = list_path.copy()
                                grouped_path[i] = group
                                grouped_path = tuple(grouped_path)
                                grouped_cols[grouped_path].append(c)
                                grouped_variables[grouped_path].append(variable)

        for group, cols in grouped_cols.items():
            col_name = 'g:' + ','.join(cols)
            data[col_name] = data[cols].aggregate(func='sum', axis=1)
            variables = grouped_variables[group]
            variable_map[col_name] = RegroupedVariable(path=group, variables=variables)
            data.drop(labels=cols, axis=1, inplace=True)

        for c, variable in variable_map.items():
            if c in data.columns:
                data[c].census.variable = variable

        return data

FIVE_RACE_REGROUPER = Regrouper(groupings={
    'other': ['some other race alone', 'two or more races', 'american indian and alaska native alone', 'native hawaiian and other pacific islander alone']
})

MAX_AGE = 120

class AgeRegrouper(Regrouper):
    def __init__(self, age_brackets: List[str]) -> None:
        self.age_brackets = age_brackets

    def regroup(self, data: DataFrame) -> DataFrame:
        age_assignments = {}
        for bracket in self.age_brackets:
            if bracket[-1] == '+':
                start = int(bracket[:-1])
                stop = MAX_AGE
            elif '-' in bracket:
                ages = bracket.split('-')
                start, stop = int(ages[0]), int(ages[1])
            else:
                raise Exception

            for i in range(start, stop + 1):
                if i not in age_assignments:
                    age_assignments[i] = bracket
                else:
                    raise Exception

        def assign_bracket(census_bracket: str, start: int = None, stop: int = None):
            try:
                if start and stop:
                    if age_assignments[start] == age_assignments[stop]:
                        return age_assignments[start]
                    else:
                        raise KeyError
                else:
                    if start:
                        return age_assignments[start]
                    elif stop:
                        return age_assignments[stop]
            except KeyError:
                raise ValueError(f"The Census age bracket '{census_bracket}' does not fit into the brackets you provided")

        groupings = defaultdict(set)
        variables = data.census.variables
        for c in data.columns:
            variable = data[c].census.variable
            if variable is not None:
                for element in variable.path:
                    match = re.match(AGE_REGEX, element)
                    if match is not None:
                        if match['under'] is not None:
                            under_end = int(match['under_end'])
                            bracket = assign_bracket(census_bracket=element, stop=under_end)
                        elif match['two'] is not None:
                            two_start = int(match['two_start'])
                            two_end = int(match['two_end'])
                            bracket = assign_bracket(census_bracket=element, start=two_start, stop=two_end)
                        elif match['to'] is not None:
                            to_start = int(match['to_start'])
                            to_end = int(match['to_end'])
                            bracket = assign_bracket(census_bracket=element, start=to_start, stop=to_end)
                        elif match['over'] is not None:
                            over_start = int(match['over_start'])
                            bracket = assign_bracket(census_bracket=element, start=over_start)
                        elif match['one'] is not None:
                            one_start = int(match['one_start'])
                            bracket = assign_bracket(census_bracket=element, start=one_start)

                        groupings[bracket].add(element)

        self.groupings = groupings

        return super().regroup(data=data)