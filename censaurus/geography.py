import warnings
from collections import defaultdict
import typing


class UnknownGeography(Exception):
    pass


class InvalidGeographyHierarchy(Exception):
    pass


class In:
    def __init__(self, geography, value) -> None:
        self.geography = geography
        self.value = self.validate(geography, value)

    # TODO: add more checking on values, lens, etc.
    @staticmethod
    def validate(geography, value):
        if isinstance(value, int):
            return str(value)
        elif isinstance(value, str):
            return value
        else:
            raise TypeError('geography value must be str or int types')


class Ins:
    def __init__(self) -> None:
        self.ins = {}
        self.has_wc = False

    def add(self, i: In):
        if i.value != '*':
            if self.has_wc:
                raise InvalidGeographyHierarchy(f"cannot specify '{i.geography}' if one of {list(self.ins.keys())} is already unspecified")
        else:
            self.has_wc = True
        self.ins[i.geography] = i.value

    def to_string(self):
        return '&'.join([f'in={g}:{v}' for g, v in self.ins.items()])


class Geography:
    def __init__(self, params: dict) -> None:
        self.name = params.get('name', None)
        self.level = params.get('geoLevelDisplay', None)
        self.requires = params.get('requires', [])
        self.wildcard = params.get('wildcard', [])
        self.optional_wildcard = params.get('optionalWithWCFor', [])
        if isinstance(self.optional_wildcard, str):
            self.optional_wildcard = [self.optional_wildcard]

        self.ordered_others = []
        for g in self.requires + self.wildcard + self.optional_wildcard:
            if g not in self.ordered_others:
                self.ordered_others += [g]

        self.readable_path = ' -> '.join(self.requires) + ' -> ' + self.name if self.requires else self.name

    def __repr__(self) -> str:
        return f'\n{self.name}\n  requires: {self.requires}\n  wildcards: {self.wildcard}\n  path: [{self.readable_path}]'

    def _build_geography_string(self, filters):
        for_str = f'for={self.name}:'
        if self.name in filters:
            for_str += filters[self.name]
        else:
            for_str += '*'
        
        unused_params = set(filters.keys())
        if self.name in unused_params:
            unused_params.remove(self.name)
        ins = Ins()
        for g in self.requires:
            if g in filters:
                ins.add(i=In(g, filters[g]))
                unused_params.remove(g)
            elif g in self.wildcard:
                ins.add(i=In(g, '*'))
            else:
                raise InvalidGeographyHierarchy(f"missing '{g}'")
        if len(unused_params) > 0:
            warnings.warn(f'{len(unused_params)} unused geography parameters: {list(unused_params)}')
        ins_str = ins.to_string()
                
        return f'{for_str}&{ins_str}'


class GeographyCollection:
    def __init__(self, supported_geographies_json) -> None:
        self.supported_geographies = defaultdict(list)

        for g in supported_geographies_json:
            self.supported_geographies[g['name']].append(Geography(g))

    def __repr__(self):
        return str(list(self.supported_geographies.values()))

    def __len__(self):
        return len(self.supported_geographies)

    def _build_geography_string(self, name, filters):
        options = self.search(name)
        for opt in options:
            try:
                return opt, opt._build_geography_string(filters)
            except InvalidGeographyHierarchy:
                pass
        raise InvalidGeographyHierarchy # TODO: need a helpful message here

    def search(self, name) -> typing.List[Geography]:
        if name in self.supported_geographies:
            return self.supported_geographies[name]
        else:
            raise UnknownGeography(name)