import warnings
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Union
import json
from geopandas import GeoDataFrame

from censaurus.graph_utils import visualize_graph
from censaurus.tiger import AreaCollection, Area
from censaurus.constants import LAYER_NAME_MAP

class UnknownGeography(Exception):
    pass


class InvalidGeographyHierarchy(Exception):
    pass


def pad_geography_filters(geo_filters: Dict[str, str]):
    # more stuff can happen here, like checking if names are given as postal codes or state names etc.
    for g, value in geo_filters.items():
        if value != '*':
            if g == 'state':
                geo_filters[g] = str(int(value)).zfill(2)
            elif g == 'county':
                geo_filters[g] = str(int(value)).zfill(3)
            elif g == 'tract':
                geo_filters[g] = str(int(value)).zfill(6)
            elif g == 'block group':
                geo_filters[g] = str(int(value))
            elif g == 'block':
                geo_filters[g] = str(int(value)).zfill(4)
            elif g == 'place':
                geo_filters[g] = str(int(value)).zfill(5)
            elif g == 'metropolitan statistical area/micropolitan statistical area':
                geo_filters[g] = str(int(value)).zfill(5)
            elif g == 'combined statistical area':
                geo_filters[g] = str(int(value)).zfill(3)
            elif g == 'congressional district':
                geo_filters[g] = str(int(value)).zfill(2)
            elif g == 'voting district':
                geo_filters[g] = str(int(value)).zfill(6)
            elif g == 'zip code tabulation area':
                geo_filters[g] = str(int(value)).zfill(5)

    return geo_filters


class In:
    def __init__(self, geography, value) -> None:
        self.geography = geography
        self.value = self.validate(geography, value)

    @staticmethod
    def validate(geography, value):
        if isinstance(value, int):
            return str(value)
        elif isinstance(value, str):
            return value
        else:
            raise TypeError('geography value must be str or int types')


class Ins:
    def __init__(self, has_wc: bool = False) -> None:
        self.ins = {}
        self.has_wc = has_wc

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
        self.params = params
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

        self.path = tuple(self.requires) + (self.name,)
        self.parent_path = self.path[:-1]
        self.readable_path = ' -> '.join(self.requires) + ' -> ' + self.name if self.requires else self.name

    def __repr__(self) -> str:
        return f'{self.name} ({self.level})\n  requires: {self.requires}\n  wildcards: {self.wildcard}\n  path: [{self.readable_path}]\n'

    def _build_broadest_params(self, feature_attributes: Dict[str, str]):
        geo_filters = {}
        for r in self.requires:
            if r not in self.wildcard:
                if r in feature_attributes:
                    geo_filters[r] = feature_attributes[r]
        try:
            return self._build_geography_params(geo_filters=geo_filters)
        except InvalidGeographyHierarchy:
            return False
    
    def _build_geography_params(self, geo_filters: Dict[str, str]):
        geo_filters = pad_geography_filters(geo_filters=geo_filters)
        geo_params = defaultdict(list)

        has_specified_geo = False
        if self.name in geo_filters and geo_filters[self.name] != '*':
            geo_params['for'].append(f'{self.name}:{geo_filters[self.name]}')
            has_specified_geo = True
            del geo_filters[self.name]
        else:
            geo_params['for'].append(f'{self.name}:*')
        
        unused_params = set(geo_filters.keys())
        reverse_requires = self.requires[::-1]
        for i, g in enumerate(reverse_requires):
            if g in geo_filters:
                value = geo_filters[g]
            elif g in self.wildcard:
                value = '*'
            else:
                raise InvalidGeographyHierarchy(f"'{g}' must supplied as a geo filter")

            if value == '*':
                if g not in self.wildcard:
                    raise InvalidGeographyHierarchy(f"'{g}' must be specified and cannot be a wildcard")
                if has_specified_geo is True:
                    raise InvalidGeographyHierarchy(f"cannot use wildcard for '{g}' because one of {reverse_requires[:i] + [self.name]} is already specified")
            else:
                has_specified_geo = True
            
            geo_params['in'].append(f'{g}:{value}')

            if g in unused_params:
                unused_params.remove(g)

        return geo_params


class GeographyCollection:
    def __init__(self, supported_geographies_json) -> None:
        self._path_to_name_map : Dict[tuple, str] = {}
        self._geography_map : Dict[str, Geography] = {}
        self._geography_tree : Dict[tuple, Set[tuple]] = {}

        for g in supported_geographies_json:
            geo = Geography(g)
            self._geography_map[geo.level] = geo
            self._path_to_name_map[geo.path] = geo.readable_path
            self._geography_tree[geo.path] = set()

        for g in self._geography_map.values():
            if g.parent_path in self._path_to_name_map:
                self._geography_tree[g.parent_path].add(g.path)

    def __iter__(self):
        return iter(self._geography_map.values())

    def __len__(self):
        return len(self._geography_map)

    def __repr__(self):
        gs = self._geography_map.values()
        return_str = ''
        for g in gs:
            return_str += str(g)
        return return_str

    def get(self, level: str = None, name: str = None) -> Union[Geography, List[Geography]]:
        if level:
            matches = [g for g in self._geography_map.values() if g.level == level]
            if len(matches) > 0:
                return matches[0]
            else:
                raise UnknownGeography(f'The requested geographic level ({level}) is not available for this dataset.')
        elif name:
            matches = [g for g in self._geography_map.values() if g.name == name]
            if len(matches) > 0:
                return matches
            else:
                raise UnknownGeography(f"The current dataset does not have geography '{name}'")

    def _build_geography_params_from_features_within(self, features_within: GeoDataFrame, target: str) -> Tuple[Geography, List[Dict[str, Union[str, List[str]]]]]:
        geographies = self.get(name=target)
        if isinstance(geographies, Geography):
            geographies = [geographies]

        geography_params_sets = []
        for geography in geographies:
            is_possible = True
            params_set = set()
            for id, feature in features_within.iterrows():
                feature_attributes = feature.to_dict()
                broadest_params = geography._build_broadest_params(feature_attributes=feature_attributes)
                if broadest_params is not False:
                    params_set.add(json.dumps(broadest_params, sort_keys=True))
                else:
                    is_possible = False
                    break
            if is_possible:
                geography_params_sets.append((geography, params_set))

        if len(geography_params_sets) == 0:
            raise Exception('this specific geography set is not possible, apologies.')

        geography_params_sets = sorted(geography_params_sets, key=lambda lps : len(lps[1]))
        best_geography = geography_params_sets[0][0]
        best_param_set = geography_params_sets[0][1]
        return best_geography, [json.loads(p) for p in best_param_set]         

    def _build_geography_params(self, areas: AreaCollection, within: Union[Area, List[Area]], target: str, target_layer_name: Union[Area, List[Area]], return_geometry: bool):
        if isinstance(within, list) and len(within) == 1:
            within = within[0]

        # TODO: error handling here for making sure that within is of a good type -- should also show how to do it?
        
        # theoretically, could do each of the areas individually if within is a list
        # what to do if return_geometry = True and within = US_xxx
        if return_geometry is False and isinstance(within, Area):
            geographies = self.get(name=target)
            if isinstance(geographies, Geography):
                geographies = [geographies]
            for geography in geographies:
                geo_filters = {}
                for r in geography.requires:
                    if r in within.attributes:
                        geo_filters[r] = within.attributes[r]
                if within.layer_name != 'US' and LAYER_NAME_MAP[within.layer_name] not in geo_filters:
                    # TODO: make sure that this is right -- generally, it makes sure that you are using the within geometry
                    continue
                try:
                    geography_params = geography._build_geography_params(geo_filters=geo_filters)
                    geography_params_list = [geography_params]
                    return geography, geography_params_list, None
                except InvalidGeographyHierarchy:
                    continue
        
        # TODO: missing area threshold here, need to figure out how to pass it
        if target_layer_name is None:
            raise ValueError('Since the geographic level you requested cannot be resolved inside a default Census hierarchy (or because you set return_geometry = True), you must specify the name of the geographic layer you want to query. To see the available options, see AreaCollection.available_layers.')

        features_within = areas.get_features_within(within=within, layer_name=target_layer_name)
        geography, geography_params_list = self._build_geography_params_from_features_within(features_within=features_within, target=target)
        return geography, geography_params_list, features_within
    
    def _build_geography_params_from_level(self, level: str = None, name: str = None, geo_filters: Dict[str, str] = {}):
        if level:
            g = self.get(level=level)
            return g, g._build_geography_params(geo_filters)
        elif name:
            matches = self.get(name=name)
            exceptions = []
            for match in matches:
                try:
                    return match, match._build_geography_params(geo_filters)
                except InvalidGeographyHierarchy as e:
                    exceptions.append(e)

            if len(matches) == 1:
                raise exceptions[0]

            exception_str = f'{len(matches)} geographies match the name you specified, but none match the filters you specified. Here are the matches and the corresponding errors:\n\n'
            for i, g in enumerate(matches):
                exception_str += str(g)
                exception_str += f'error: {exceptions[i]}\n\n'

            exception_str += f'Please examine the above errors to determine which one of the {len(matches)} geographies you were trying to match, and which filters are missing.\n'

            raise InvalidGeographyHierarchy(exception_str)

    def visualize(self, label_type: str = 'difference', hierarchical: bool = False, filename: str = 'geography_graph.html'):
        if label_type == 'name':
            labels = {g.path: g.readable_path for g in self._geography_map.values()}
        elif label_type == 'difference':
            labels = {g.path: g.path[-1] for g in self._geography_map.values()}

        titles = {g.path: str(g) for g in self._geography_map.values()}

        visualize_graph(tree=self._geography_tree, titles=titles, labels=labels, hierarchical=hierarchical, filename=filename)

    def to_list(self):
        return list(self._geography_map.values())