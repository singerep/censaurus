from typing import Dict, List, Iterable
from pandas import DataFrame
from collections import defaultdict
from re import match

from censaurus.variable import RegroupedVariable
from censaurus.rename import AGE_REGEX


class Regrouper:
    """
    An object to handle regrouping Census variables.

    Parameters
    ==========
    groupings : :obj:`dict` of :obj:`str`: array-like of :obj:`str`
        Each key in this dictionary is the name of a new group. Each corresponding value
        should be a list of elements that fall into that group. For example,
        ``groupings={"white_or_black": ["white alone", "black or african american alone"]}``
        would aggregate all columns that have the token ``white alone`` or the token
        ``black or african american alone``, and create a new column with those tokens
        replaces as ``white_or_black``.
    """
    def __init__(self, groupings: Dict[str, Iterable[str]] = {}) -> None:
        self.groupings = groupings

    def regroup(self, data: DataFrame):
        """
        Regroups the columns of the dataset. Note that you should probably rename
        the data after a regrouping with a :class:`.Renamer` object to avoid long
        column names.

        Parameters
        ==========
        data : :class:`pandas.DataFrame`
            The data to regroup.
        """
        variables = data.census.variables

        variable_map = {}
        column_map = {}
        grouped_cols = defaultdict(set)
        grouped_variables = defaultdict(list)
        
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
                                grouped_cols[grouped_path].add(c)
                                grouped_variables[grouped_path].append(variable)

        for group, cols in grouped_cols.items():
            col_places = zip(cols, range(0, len(cols)))
            last_col = sorted(col_places, key=lambda c : c[1], reverse=True)[0][0]
            col_name = 'g:' + ','.join(sorted(cols))
            data[last_col] = data[list(cols)].aggregate(func='sum', axis=1)
            data.rename(columns={last_col: col_name}, inplace=True)
            cols.remove(last_col)
            variables = grouped_variables[group]
            variable_map[col_name] = RegroupedVariable(path=group, variables=variables)
            data.drop(labels=list(cols), axis=1, inplace=True)

        for c, variable in variable_map.items():
            if c in data.columns:
                data[c].census.variable = variable

        return data

FIVE_RACE_REGROUPER = Regrouper(groupings={
    'other': ['some other race alone', 'two or more races', 'american indian and alaska native alone', 'native hawaiian and other pacific islander alone']
})
"""A :class:`.Regrouper` object that aggregates ``some other race alone``, ``two or more 
races``, ``american indian and alaska native alone``, ``native hawaiian and other pacific
islander alone`` into a group called ``other``."""

MAX_AGE = 120

class AgeRegrouper(Regrouper):
    """
    A subclass of the :class:`.Regouper` class that specifically handles regrouping
    into new age buckets.

    Parameters
    ==========
    age_brackets : :obj:`list` of :obj:`str`
        The new age brackets to group into. Must be of the form 
        ``"<start_year>-<end_year>"``, except for the oldest age bracket, which should
        be of the form ``<start_year>+``. For example, 
        ``age_brackets=["0-17", "18-29", "30-49", "50-64", "65+"]``.
    """
    def __init__(self, age_brackets: List[str]) -> None:
        self.age_brackets = age_brackets

    def regroup(self, data: DataFrame) -> DataFrame:
        """
        Regroups the columns of the dataset. Note that you should probably rename
        the data after a regrouping with a :class:`.Renamer` object to avoid long
        column names.

        Parameters
        ==========
        data : :class:`pandas.DataFrame`
            The data to regroup.
        """
        age_assignments = {}
        for bracket in self.age_brackets:
            if bracket[-1] == '+':
                start = int(bracket[:-1])
                stop = MAX_AGE
            elif '-' in bracket:
                ages = bracket.split('-')
                start, stop = int(ages[0]), int(ages[1])
            else:
                raise ValueError('Each age bracket should be formatted like <start>-<stop> or <start>+')

            for i in range(start, stop + 1):
                if i not in age_assignments:
                    age_assignments[i] = bracket
                else:
                    raise ValueError(f"The age {i} has been assigned to more than one age bracket")

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
        for c in data.columns:
            variable = data[c].census.variable
            if variable is not None:
                for element in variable.path:
                    age_match = match(AGE_REGEX, element)
                    if age_match is not None:
                        if age_match['under'] is not None:
                            under_end = int(age_match['under_end'])
                            bracket = assign_bracket(census_bracket=element, stop=under_end)
                        elif age_match['two'] is not None:
                            two_start = int(age_match['two_start'])
                            two_end = int(age_match['two_end'])
                            bracket = assign_bracket(census_bracket=element, start=two_start, stop=two_end)
                        elif age_match['to'] is not None:
                            to_start = int(age_match['to_start'])
                            to_end = int(age_match['to_end'])
                            bracket = assign_bracket(census_bracket=element, start=to_start, stop=to_end)
                        elif age_match['over'] is not None:
                            over_start = int(age_match['over_start'])
                            bracket = assign_bracket(census_bracket=element, start=over_start)
                        elif age_match['one'] is not None:
                            one_start = int(age_match['one_start'])
                            bracket = assign_bracket(census_bracket=element, start=one_start)

                        groupings[bracket].add(element)

        self.groupings = groupings

        return super().regroup(data=data)