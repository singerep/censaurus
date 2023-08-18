from typing import Dict, Callable
from re import Match, finditer
from pandas import DataFrame

import censaurus.census_accessors
from censaurus.variable import Variable
from censaurus.dataset import Dataset


class Renamer:
    """
    An object that handles renaming Census variable names in datasets.

    Parameters
    ==========
    separator : :obj:`str` = '|'
        The string to use to join individual tokens from the label of a 
        :class:`.Variable`.
    default_rename_function : :obj:`function` of :obj:`str` -> :obj:`str` = ``lambda x : x``
        The default function to pass individual tokens from the label of a 
        :class:`.Variable` into. Defaults to simply returning the token.
    replacements : :obj:`dict` of :obj:`str`::obj:`str` = {}
        Individual tokens to search for (keys of the dictionary) and replace (values of
        the dictionary).
    custom_match_functions : :obj:`dict` of :obj:`str`: :obj:`function` of (:class:`re.Match`, :obj:`str`) -> :obj:`str` = {}
        A dictionary where each key represents to a renaming function that takes as an
        input a match and a string and returns a string. The individual tokens from the 
        label of a :class:`.Variable` are checked against each key in this dictionary:
        if there is a match, the corresponding renaming function is used.
    group_prefixes : :obj:`dict` of :obj:`str`: :obj:`str` = {}
        If a :class:`.Variable` is in a group that matches to a key in this dictionary,
        the prefix of the column name after the renaming will be the corresponding
        value in the dictionary as opposed to that group's concept (the default 
        behavior).
    """
    def __init__(self, separator: str = '|', default_rename_function: Callable[[str], str] = lambda x : x, replacements: Dict[str, str] = {}, custom_match_functions: Dict[str, Callable[[Match, str], str]] = {}, group_prefixes: Dict[str, str] = {}) -> None:
        self._separator = separator
        self.replacements = replacements
        self.default_rename_function = default_rename_function
        self.custom_match_functions = custom_match_functions
        self.group_prefixes = group_prefixes

    @property
    def separator(self):
        return self._separator

    @separator.setter
    def separator(self, separator: str):
        self._separator = separator

    def add_group_prefixes(self, group_prefixes: Dict[str, str]) -> None:
        """
        Adds additional group prefixes to an existing :class:`.Renamer`.

        Parameters
        ==========
        group_prefixes : :obj:`dict` of :obj:`str`: :obj:`str` = {}
            The groups and prefixes to add.
        """
        self.group_prefixes.update(group_prefixes)

    def _rename_variable(self, variable: Variable) -> str:
        group = variable.group
        concept = variable.concept
        if group is not None:
            tokens = list(variable.path[1:])
        else:
            tokens = list(variable.path)
        if group in self.group_prefixes:
            tokens = [self.group_prefixes[group]] + tokens
        else:
            tokens = [concept] + tokens

        for i, token in enumerate(tokens):
            if token not in self.replacements:
                new_token = token
                for pattern, function in self.custom_match_functions.items():
                    if new_token is not None:
                        for match in finditer(pattern=pattern, string=new_token):
                            new_token = function(match, new_token)

                new_token = self.default_rename_function(new_token)
            else:
                new_token = self.replacements[token]
            tokens[i] = new_token
        
        return self.separator.join([t for t in tokens if t])

    def rename(self, data: DataFrame):
        """
        Renames the columns of the dataset.

        Parameters
        ==========
        data : :class:`pandas.DataFrame`
            The data to rename.
        """
        new_name_map = {}
        old_name_map = {}
        variable_map = {}
        for c in data.columns:
            v = data[c].census.variable
            variable_map[c] = v
            if v is not None:
                new_name = self._rename_variable(variable=v)
                new_name_map[c] = new_name
                old_name_map[new_name] = c

        data.rename(columns=new_name_map, inplace=True)

        for new_name, old_name in old_name_map.items():
            data[new_name].census.variable = variable_map[old_name]

        return data

def _age_renamer(match: Match, token: str) -> str:
    start, stop = match.span()
    original_match = match.string[start:stop]
    if match['under'] is not None:
        under_end = match['under_end']
        return token.replace(original_match, f'0-{under_end}')
    elif match['two'] is not None:
        two_start = match['two_start']
        two_end = match['two_end']
        return token.replace(original_match, f'{two_start}-{two_end}')
    elif match['to'] is not None:
        to_start = match['to_start']
        to_end = match['to_end']
        return token.replace(original_match, f'{to_start}-{to_end}')
    elif match['over'] is not None:
        over_start = match['over_start']
        return token.replace(original_match, f'{over_start}+')
    elif match['one'] is not None:
        one_start = match['one_start']
        return token.replace(original_match, f'{one_start}')
    return None

def _type_renamer(match: Match, token: str) -> str:
    if token == 'estimate':
        return None
    elif token == 'margin of error':
        return 'moe'
    elif token == 'annotation of estimate':
        return 'ae'
    elif token == 'annotation of margin of error':
        return 'amoe'
    return None

def _race_renamer(match: Match, token: str) -> str:
    start, stop = match.span()
    original_match = match.string[start:stop]
    if match['white'] is not None:
        return token.replace(original_match, 'white')
    elif match['black'] is not None:
        return token.replace(original_match, 'black')
    elif match['aian'] is not None:
        return token.replace(original_match, 'AIAN')
    elif match['asian'] is not None:
        return token.replace(original_match, 'asian')
    elif match['nhpi'] is not None:
        return token.replace(original_match, 'NHPI')
    elif match['other'] is not None:
        return token.replace(original_match, 'other')
    elif match['two'] is not None:
        return token.replace(original_match, '2')
    elif match['two_plus'] is not None:
        return token.replace(original_match, '2+')
    elif match['three_plus'] is not None:
        return token.replace(original_match, '3+')
    elif match['alone'] is not None:
        return token.replace(original_match, '')
    elif match['hisp'] is not None:
        return token.replace(original_match, 'hisp')
    else:
        return token

def _inflation_renamer(match: Match, token: str) -> str:
    start, stop = match.span()
    original_match = match.string[start:stop]
    year = match['year']
    return token.replace(original_match, f'${year}')

def _income_renamer(match: Match, token: str) -> str:
    start, stop = match.span()
    original_match = match.string[start:stop]
    if match['less_than'] is not None:
        less_than_stop = int(''.join(match['less_than_stop'].split(',')))
        return token.replace(original_match, f'$0-{less_than_stop}')
    elif match['to'] is not None:
        to_start = int(''.join(match['to_start'].split(',')))
        to_stop = int(''.join(match['to_stop'].split(',')))
        return token.replace(original_match, f'${to_start}-{to_stop}')
    elif match['or_more'] is not None:
        or_more_start = int(''.join(match['or_more_start'].split(',')))
        return token.replace(original_match, f'${or_more_start}+')

AGE_REGEX = r'((?P<under>under (?P<under_end>\d+) years)|(?P<two>(?P<two_start>\d+) and (?P<two_end>\d+) years)|(?P<to>(?P<to_start>\d+) to (?P<to_end>\d+) years)|(?P<over>(?P<over_start>\d+) years and over)|(?P<one>(?P<one_start>\d+) years))'
RACE_REGEX = r'((?P<white>white)|(?P<black>black or african american)|(?P<aian>american indian and alaska native)|(?P<asian>asian)|(?P<nhpi>native hawaiian and other pacific islander)|(?P<other>some other race)|(?P<two_plus>two or more races)|(?P<two>two races)|(?P<three_plus>three or more races)|(?P<hisp>hispanic or latino)|(?P<alone> alone))'
TYPE_REGEX = r'((estimate)|(margin of error)|(annotation of estimate)|(annotation of margin of error))'
INFLATION_REGEX = r'in (?P<year>\d+) inflation-adjusted dollars'
INCOME_REGEX = r'((?P<less_than>less than \$(?P<less_than_stop>[0-9,]+))|(?P<to>\$(?P<to_start>[0-9,]+) to \$(?P<to_stop>[0-9,]+))|(?P<or_more>\$(?P<or_more_start>[0-9,]+) or more))'

SIMPLE_RENAMER = Renamer(
    default_rename_function=lambda x : ' '.join(x.split()) if x is not None else None, 
    custom_match_functions={
        AGE_REGEX: _age_renamer,
        RACE_REGEX: _race_renamer,
        TYPE_REGEX: _type_renamer,
        INFLATION_REGEX: _inflation_renamer,
        INCOME_REGEX: _income_renamer
    }
)
"""A :class:`.Renamer` object that renames variables in a simple way. Has custom match
functions to handle: age, race, variable types, inflation, and incomes."""
