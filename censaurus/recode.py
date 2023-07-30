import pandas as pd
from itertools import combinations


class RecodeError(Exception):
    ...


class StateRecoder:
    def __init__(self) -> None:
        self.types = {'FIPS', 'FIPS_PADDED', 'ABBR', 'NAME', 'GNIS'}
        self.type_explanations = {
            'FIPS': 'Integer codes. For example: 1 for Alabama, 2 for Alaska, etc.',
            'FIPS_PADDED': '0-padded two digit integer codes. For example: 01 for Alabama, 02 for Alaska, etc.',
            'ABBR': 'Two character state abbreviations (postal codes). For example: AL for Alabama, AK for Alaska, etc.',
            'NAME': 'Full state names. For example: Alabama, Alaska, etc.',
            'GNIS': 'Geographic Names Information System identifiers. For example: 01779775 for Alabama, 01785533 for Alaska, etc.',
        }

        state_ids = pd.read_csv('censaurus/data/state_ids.csv')
        state_ids['FIPS_PADDED'] = state_ids['FIPS'].apply(lambda f : str(f).zfill(2))
        self.recode_dicts = {}
        for from_type, to_type in combinations(iterable=self.types, r=2):
            self.recode_dicts[f'{from_type}_{to_type}'] = dict(zip(state_ids[from_type].astype(str), state_ids[to_type].astype(str)))
            self.recode_dicts[f'{to_type}_{from_type}'] = dict(zip(state_ids[to_type].astype(str), state_ids[from_type].astype(str)))

    def to_new(self, data: pd.DataFrame, new_type: str, state_col: str = 'state'):
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

    def to_FIPS(self, data: pd.DataFrame, state_col: str = 'state'):
        return self.to_new(data=data, new_type='FIPS', state_col=state_col)

    def to_FIPS_PADDED(self, data: pd.DataFrame, state_col: str = 'state'):
        return self.to_new(data=data, new_type='FIPS_PADDED', state_col=state_col)

    def to_ABBR(self, data: pd.DataFrame, state_col: str = 'state'):
        return self.to_new(data=data, new_type='ABBR', state_col=state_col)

    def to_NAME(self, data: pd.DataFrame, state_col: str = 'state'):
        return self.to_new(data=data, new_type='NAME', state_col=state_col)

    def to_GNIS(self, data: pd.DataFrame, state_col: str = 'state'):
        return self.to_new(data=data, new_type='GNIS', state_col=state_col)