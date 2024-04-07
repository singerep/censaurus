from pandas import DataFrame, read_csv
from itertools import combinations
from censaurus.internals import state_ids_path


class RecodeError(Exception):
    ...


class StateRecoder:
    """
    An object that handles recoding state to various formats. The available formats are:
    
       + ``FIPS``: FIPS codes with no zero-padding (1, 2, 4, etc.)
       + ``FIPS_PADDED``: FIPS codes with two-digit zero-padding (01, 02, 04, etc.)
       + ``ABBR``: state abbreviations (AL, AK, AZ, etc.)
       + ``NAME``: full state names (Alabama, Alaska, Arizona, etc.)
       + ``GNIS``: Geographic Names Information System codes (1779775, 1785533, 1779777 etc.)
       + ``GNIS_PADDED``: Geographic Names Information System codes with seven-digit zero-padding (1779775, 1785533, 1779777 etc.)

    """
    def __init__(self) -> None:
        self.types = {'FIPS', 'FIPS_PADDED', 'ABBR', 'NAME', 'GNIS', 'GNIS_PADDED'}
        self.type_explanations = {
            'FIPS': 'Integer codes. For example: 1 for Alabama, 2 for Alaska, etc.',
            'FIPS_PADDED': '0-padded two-digit integer codes. For example: 01 for Alabama, 02 for Alaska, etc.',
            'ABBR': 'Two character state abbreviations (postal codes). For example: AL for Alabama, AK for Alaska, etc.',
            'NAME': 'Full state names. For example: Alabama, Alaska, etc.',
            'GNIS': 'Geographic Names Information System identifiers. For example: 1779775 for Alabama, 1785533 for Alaska, etc.',
            'GNIS_PADDED': '0-padded seven-digit integer codes. For example: 1779775 for Alabama, 1785533 for Alaska, etc.'
        }

        state_ids = read_csv(state_ids_path())
        state_ids['FIPS_PADDED'] = state_ids['FIPS'].apply(lambda f : str(f).zfill(2))
        state_ids['GNIS_PADDED'] = state_ids['GNIS'].apply(lambda g : str(g).zfill(7))
        self.recode_dicts = {}
        for from_type, to_type in combinations(iterable=self.types, r=2):
            self.recode_dicts[f'{from_type}_{to_type}'] = dict(zip(state_ids[from_type].astype(str), state_ids[to_type].astype(str)))
            self.recode_dicts[f'{to_type}_{from_type}'] = dict(zip(state_ids[to_type].astype(str), state_ids[from_type].astype(str)))

    def _to_new(self, data: DataFrame, new_type: str, state_col: str = 'state'):
        other_types = self.types.copy()
        other_types.remove(new_type)
        for type in other_types:
            record_dict = self.recode_dicts[f'{type}_{new_type}']
            if all(str(s) in record_dict.keys() for s in data[state_col].values):
                data[state_col] = data[state_col].astype(str).map(record_dict)
                return data

        exception_string = 'Unable to match your state identifiers to any format. Please make sure you are following one of the following formats.\n\n'
        for type in other_types:
            explanation = self.type_explanations[type]
            exception_string += f'  - {type}: {explanation}\n'

        raise RecodeError(exception_string)

    def to_FIPS(self, data: DataFrame, state_col: str = 'state'):
        """
        Recodes states as FIPS codes (1, 2, 4, etc.). Attempts to infer the 
        original state format. 

        Parameters
        ==========
        data : :class:`pandas.DataFrame`
            The data with a column to recode.
        state_col : :obj:`str` = 'state'
            The column in the dataset that list states.
        """
        return self._to_new(data=data, new_type='FIPS', state_col=state_col)

    def to_FIPS_PADDED(self, data: DataFrame, state_col: str = 'state'):
        """
        Recodes states as two-digit zero-padded FIPS codes (01, 02, 04, etc.). 
        Attempts to infer the original state format. 

        Parameters
        ==========
        data : :class:`pandas.DataFrame`
            The data with a column to recode.
        state_col : :obj:`str` = 'state'
            The column in the dataset that list states.
        """
        return self._to_new(data=data, new_type='FIPS_PADDED', state_col=state_col)

    def to_ABBR(self, data: DataFrame, state_col: str = 'state'):
        """
        Recodes states as abbreviations (AL, AK, AZ, etc.). Attempts to infer the 
        original state format. 

        Parameters
        ==========
        data : :class:`pandas.DataFrame`
            The data with a column to recode.
        state_col : :obj:`str` = 'state'
            The column in the dataset that list states.
        """
        return self._to_new(data=data, new_type='ABBR', state_col=state_col)

    def to_NAME(self, data: DataFrame, state_col: str = 'state'):
        """
        Recodes states as full names (Alabama, Alaska, Arizona, etc.). Attempts to infer 
        the original state format. 

        Parameters
        ==========
        data : :class:`pandas.DataFrame`
            The data with a column to recode.
        state_col : :obj:`str` = 'state'
            The column in the dataset that list states.
        """
        return self._to_new(data=data, new_type='NAME', state_col=state_col)

    def to_GNIS(self, data: DataFrame, state_col: str = 'state'):
        """
        Recodes states as GNIS codes (1779775, 1785533, 1779777 etc.). Attempts to 
        infer the original state format. 

        Parameters
        ==========
        data : :class:`pandas.DataFrame`
            The data with a column to recode.
        state_col : :obj:`str` = 'state'
            The column in the dataset that list states.
        """
        return self._to_new(data=data, new_type='GNIS', state_col=state_col)

    def to_GNIS_PADDED(self, data: DataFrame, state_col: str = 'state'):
        """
        Recodes states as GNIS codes (1779775, 1785533, 1779777 etc.). Attempts to 
        infer the original state format. 

        Parameters
        ==========
        data : :class:`pandas.DataFrame`
            The data with a column to recode.
        state_col : :obj:`str` = 'state'
            The column in the dataset that list states.
        """
        return self._to_new(data=data, new_type='GNIS_PADDED', state_col=state_col)