from typing import Dict, List, Union, Iterable, Set, Callable, Tuple
from shapely import intersection
from shapely.geometry import shape
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from thefuzz import process, fuzz
from shapely import union_all
import re
from Levenshtein import distance, ratio
from scipy.optimize import linear_sum_assignment
import time
from collections import defaultdict
from numpy import log

from censaurus.api import TIGERClient
from censaurus.constants import LAYER_RESULT_COUNT_MAP, FEATURE_ATTRIBUTE_MAP, FIPS_TO_ABBR, ABBR_TO_FULL, FIPS_TO_FULL, ABBR_TO_FULL_REGEX

def parse_name(name: str) -> str:
    for match in re.finditer(pattern=ABBR_TO_FULL_REGEX, string=name):
        state_abbr = match.string[match.start():match.end()]
        name = name.replace(state_abbr, ABBR_TO_FULL[state_abbr.upper()])

    name = re.sub(pattern='(?<!\d)0+(?=\d)', repl='', string=name)
    
    return name

def generate_detailed_name(feature: pd.Series, layer_name: str) -> str:
    feature_attributes = feature.to_dict()

    feature_name = feature_attributes['NAME']
    detailed_name = parse_name(name=feature_name)

    if 'state' in feature_attributes and layer_name != 'States':
        state_full = FIPS_TO_FULL[feature_attributes['state']]
        detailed_name += f', {state_full}'
    
    return detailed_name

def tokenize_feature_name(feature_name: str) -> set:
    return set(re.split(pattern='\W+', string=feature_name))

def build_custom_scorer(count_map: Dict[str, float], N: int) -> Tuple[Callable[[str, str], float], Dict[str, float]]:
    idf_map = {k: log(N/v) for k, v in count_map.items()}
    
    def token_distance_idf(s1t: str, s2t: str) -> float:
        if s1t == '' or s2t == '':
            non_empty_t = s1t if s1t != '' else s2t
            if non_empty_t in idf_map:
                return idf_map[non_empty_t]/2
        return distance(s1=s1t, s2=s2t)

    def set_distance(s1ts: Set[str], s2ts: Set[str]) -> float:
        s1tl = list(s1ts)
        s2tl = list(s2ts)

        ls1t = sum(len(s1t) for s1t in s1ts)
        ls2t = sum(len(s2t) for s2t in s2ts)

        n = len(s1ts)
        m = len(s2ts)
        k = max(n, m)

        s1tl = s1tl + ['']*(k-n)
        s2tl = s2tl + ['']*(k-m)

        ld_arr = [[token_distance_idf(s1t, s2t) for s2t in s2tl] for s1t in s1tl]
        row_assignments, col_assignments = linear_sum_assignment(ld_arr)
        sld = sum(row[i] for i, row in zip(col_assignments, ld_arr))
        nsld = (2*sld)/(ls1t + ls2t + sld)
        return nsld

    def custom_scorer(s1: str, s2: str) -> float:
        s1ts = tokenize_feature_name(s1)
        s2ts = tokenize_feature_name(s2)

        if len(s1ts) == 1 and len(s2ts) == 1:
            return ratio(s1=s1, s2=s2)
        else:
            return 1-set_distance(s1ts=s1ts, s2ts=s2ts)

    return custom_scorer, idf_map


class Area:
    def __init__(self, geo_id: str, layer_id: int, layer_name: str, tiger_client: TIGERClient, cb: bool = True) -> None:
        self.geo_id : str = geo_id
        self.name : str = None
        self.base_name : str = None
        self.attributes : Dict[str, str] = {}
        self.layer_id = layer_id
        self.layer_name = layer_name
        self.geometry : Union[Polygon, MultiPolygon] = None
        self.tiger_client = tiger_client
        self.cb = cb

    def __repr__(self) -> str:
        return f'{self.name} (GEOID={self.geo_id})'

    def _set_attributes_from_tiger(self):
        params = {
            'where': f"GEOID='{self.geo_id}'",
            'outFields': '*',
            'returnGeometry': 'false'
        }
        area_resp = self.tiger_client.get_sync(f'{self.layer_id}/query', params=params, return_type='geojson')
        feature = area_resp.json()['features'][0]

        for attr, val in feature['properties'].items():
            if attr == 'NAME' or attr == 'BASENAME':
                continue
            
            attr = FEATURE_ATTRIBUTE_MAP.get(attr, attr)
            self.attributes[attr] = val

        self.name = feature['properties']['NAME']
        self.base_name = feature['properties']['BASENAME']
        

    def _set_geometry_from_tiger(self):
        if self.geometry is not None:
            return
        
        params = {
            'where': f"GEOID='{self.geo_id}'",
            'outFields': '',
            'returnGeometry': 'true',
            'geometryPrecision': '6',
            'outSR': '4236'
        }
        area_resp = self.tiger_client.get_sync(f'{self.layer_id}/query', params=params, return_type='geojson')
        feature = area_resp.json()['features'][0]

        self.geometry = shape(feature['geometry'])
        if self.cb:
            self.geometry = intersection(US_CARTOGRAPHIC.geometry, self.geometry)

    def plot(self) -> None:
        if self.geometry is None:
            self._set_geometry_from_tiger()
        
        if self.name == 'Maryland':
            fc = 'b'
        else:
            fc = 'r'
        if self.geometry is None:
            self._set_geometry_from_tiger()
        if type(self.geometry) == Polygon:
            x, y = self.geometry.exterior.xy
            plt.fill(x, y, alpha=0.5, fc=fc, ec='none')
        elif type(self.geometry) == MultiPolygon:
            for g in self.geometry.geoms:
                x, y = g.exterior.xy
                plt.fill(x, y, alpha=0.5, fc=fc, ec='none')
        # plt.axis('equal')
        # plt.show()


US_CARTOGRAPHIC = Area(geo_id='US', layer_id=None, layer_name=None, tiger_client=None, cb=True)
US_CARTOGRAPHIC.name = 'United States'
US_CARTOGRAPHIC.base_name = 'United States'
US_CARTOGRAPHIC.layer_name = 'US'
# US_CARTOGRAPHIC.geometry = gpd.read_file('https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_nation_5m.zip')['geometry'].values[0]


class Layer:
    def __init__(self, info: Dict, tiger_client: TIGERClient) -> None:
        self.name = info['name']
        self.id = info['id']
        self.fields = [f['name'] for f in info.get('fields')]

        self.tiger_client = tiger_client

    def __repr__(self) -> str:
        return f'MapService Layer ({self.name})'

    def _get_feature_attributes(self, bbox: Iterable[float] = None, out_fields: str = '*') -> gpd.GeoDataFrame:
        params = {
            'where': '1=1',
            'outFields': out_fields,
            'returnGeometry': 'false'
        }
        if bbox:
            params.update({
                'geometry': ','.join(str(b) for b in bbox),
                'geometryType': 'esriGeometryEnvelope',
                'inSR': '4236',
                'spatialRel': 'esriSpatialRelIntersects'
            })

        features_resp = self.tiger_client.get_sync(url=f'{self.id}/query', params=params, return_type='geojson')
        features = features_resp.json()['features']
        return gpd.GeoDataFrame.from_features(features=features)

    def _get_feature_geometry(self, bbox: Iterable[float] = None, feature_count: int = None, cb: bool = True) -> gpd.GeoDataFrame:
        params_list = []

        params = {
            'where': '1=1',
            'outFields': 'GEOID',
            'returnGeometry': 'true',
            'geometryPrecision': '6',
            'outSR': '4236'
        }

        if bbox:
            params.update({
                'geometry': ','.join(str(b) for b in bbox),
                'geometryType': 'esriGeometryEnvelope',
                'inSR': '4236',
                'spatialRel': 'esriSpatialRelIntersects'
            })

        if feature_count is None:
            # TODO: send a canary with the returnCountOnly param set to true
            raise NotImplementedError

        if self.name in LAYER_RESULT_COUNT_MAP:
            result_record_count = LAYER_RESULT_COUNT_MAP[self.name]
        else:
            result_record_count = 100

        for i in range(1 + (feature_count//result_record_count)):
            result_offset = i*result_record_count
            params = params.copy()
            params['resultRecordCount'] = result_record_count
            params['resultOffset'] = result_offset
            params_list.append(params)

        urls_list = [f'{self.id}/query']*len(params_list)
        url_params_list = zip(urls_list, params_list)
        
        features_responses = self.tiger_client.get_many_sync(url_params_list=url_params_list, return_type='geojson')

        gdfs = []
        for features_resp in features_responses:
            features = features_resp.json()['features']
            gdf = gpd.GeoDataFrame.from_features(features=features)
            gdfs.append(gdf)

        features = gpd.GeoDataFrame(pd.concat(gdfs))
        features = features.reset_index()
        return features

    def get_features(self, bbox: Iterable[float] = None, out_fields: str = '*', return_geometry: bool = False, cb: bool = True) -> gpd.GeoDataFrame:
        features = self._get_feature_attributes(bbox=bbox, out_fields=out_fields)
        if return_geometry:
            geometries = self._get_feature_geometry(bbox=bbox, feature_count=len(features), cb=cb)
            features = gpd.GeoDataFrame(features.drop(labels=['geometry'], axis=1).merge(geometries, on='GEOID', how='inner'))
        features = features.rename(columns=FEATURE_ATTRIBUTE_MAP)
        return features

    def get_area_by_geo_id(self, geoid: str, cb: bool = True) -> Area:
        geoid = str(geoid)
        features = self.get_features(out_fields='GEOID')
        geoids = features['GEOID'].values
        if geoid in geoids:
            print(f"successfully matched GEOID = {geoid} in layer '{self.name}'")
            # area = Area(geo_id=geoid, layer_id=self.id, layer_name=self.name, tiger_client=self.tiger_client, cb=cb)
            # area._set_attributes_from_tiger()
            # return area
            return
        
        exception_string = f"The GEOID '{geoid}' does not have any matches within the layer '{self.name}'. Please ensure you are searching at the correct geographic level. For reference, here are some examples of GEOIDs within this layer.\n"
        for geoid in geoids[:5]:
            exception_string += f'  - {geoid}\n'
        raise Exception(exception_string)

    def get_area_by_name(self, name: str, cb: bool = True) -> Area:
        parsed_name = parse_name(name=name)
        parsed_name_token_set = tokenize_feature_name(feature_name=parsed_name)
        
        features = self.get_features()
        
        features['detailed_name'] = features.apply(func=lambda f : generate_detailed_name(feature=f, layer_name=self.name), axis=1)
        token_sets = features['detailed_name'].apply(tokenize_feature_name).to_list()
        N = len(token_sets)
        count_map = defaultdict(int)
        for token_set in token_sets:
            for token in token_set:
                count_map[token.lower()] += 1
        
        custom_scorer, idf_map = build_custom_scorer(count_map=count_map, N=N)
        
        geoid_name_dict = dict(zip(features['GEOID'], features['detailed_name']))
        geoid_token_set_dict = dict(zip(features['GEOID'], token_sets))
        best_matches = process.extractBests(query=parsed_name, choices=geoid_name_dict, scorer=custom_scorer, limit=20, score_cutoff=0.8)
        if len(best_matches) == 0:
            exception_string = f"The name '{name}' does not have any matches within the layer '{self.name}'. Please ensure you've spelled everything correctly and are searching at the correct geographic level. For reference, here are some examples of names within this layer.\n"
            example_names = features['detailed_name'].to_list()[:5]
            for example in example_names:
                exception_string += f'  - {example}\n'
            raise Exception(exception_string)
        
        elif len(best_matches) == 1 or best_matches[0][1] >= 0.99:
            geoid = best_matches[0][2]
            area_name = best_matches[0][0]
            print(f"successfully matched '{name}' to '{area_name}' (GEOID = {geoid}) in layer '{self.name}'")
            area = Area(geo_id=geoid, layer_id=self.id, layer_name=self.name, tiger_client=self.tiger_client, cb=cb)
            area._set_attributes_from_tiger()
            return area
        
        elif best_matches[0][1] > 0.95 and best_matches[0][1] - best_matches[1][1] > 0.05:
            geoid = best_matches[0][2]
            area_name = best_matches[0][0]
            print(f"matched to '{area_name}' (GEOID = {geoid}) in layer '{self.name}'")
            return
        else:
            best_match_token_sets = [geoid_token_set_dict[bm[2]] for bm in best_matches]
            best_match_geoid_name_dict = {bm[2]: geoid_name_dict[bm[2]] for bm in best_matches}
            all_missing_tokens = set([t.lower() for t in set.union(*best_match_token_sets).difference(parsed_name_token_set)])

            new_matches_found = 0
            for token in all_missing_tokens:
                new_parsed_name_token_set = parsed_name_token_set.copy()
                new_parsed_name_token_set.add(token)
                new_parsed_name = ' '.join(new_parsed_name_token_set)
                new_best_matches = process.extractBests(query=new_parsed_name, choices=best_match_geoid_name_dict, scorer=custom_scorer, limit=20, score_cutoff=0.8)

                if (len(new_best_matches) == 1 and new_best_matches[0][1] > 0.95) or (len(new_best_matches) > 0 and new_best_matches[0][1] >= 0.98):
                    geoid = new_best_matches[0][2]
                    area_name = new_best_matches[0][0]
                    new_matches_found += 1
            
            if new_matches_found == 1:
                print(f"matched to '{area_name}' (GEOID = {geoid}) in layer '{self.name}'")
                area = Area(geo_id=geoid, layer_id=self.id, layer_name=self.name, tiger_client=self.tiger_client, cb=cb)
                area._set_attributes_from_tiger()
                return area

        exception_string = f"The name '{name}' is ambiguous. Is there a typo? Could you be more specific? Did you mean any of the following (in no particular order)?\n"
        for match in best_matches[:10]:
            exception_string += f'  - {match[0]} (GEOID = {match[2]})\n'
        raise Exception(exception_string)


class AreaCollection:
    def __init__(self, map_service: str = 'tigerWMS_Current') -> None:
        self.map_service = map_service
        self.tiger_client = TIGERClient(map_service=map_service)
        self.available_layers = self._find_available_layers()

    def _find_available_layers(self) -> None:
        available_layers = {}
        layers_response = self.tiger_client.get_sync(f'layers')
        layers = layers_response.json()['layers']
        for l in layers:
            if 'Labels' not in l['name']:
                layer = Layer(l, tiger_client=self.tiger_client)
                available_layers[layer.name] = layer
        return available_layers

    def get_layer(self, layer_name: str) -> Layer:
        match, score = process.extractOne(query=layer_name, choices=self.available_layers.keys())
        if score >= 90:
            return self.available_layers[match]
        else:
            raise ValueError(f"The layer '{layer_name}' is not available for this dataset. To see the available layers, see AreaCollection.available_layers.")

    def get_features_within(self, within: Union[Area, List[Area]], layer_name: Union[str, List[str]], area_threshold: float = 0.001):
        if within == US_CARTOGRAPHIC:
            features = pd.DataFrame([{'GEOID': '0100000US'}])
            features['geometry'] = within.geometry
            return gpd.GeoDataFrame(features)

        if isinstance(within, Area):
            within = [within]

        if isinstance(layer_name, str):
            layer_name = [layer_name]

        for area in within:
            area._set_geometry_from_tiger()
        
        within_union = union_all(geometries=[a.geometry for a in within])

        features_dfs = []
        for area in within:
            bounds = area.geometry.bounds
            for name in layer_name:
                layer = self.get_layer(layer_name=name)
                features_within_bounds = layer.get_features(bbox=bounds, return_geometry=True, cb=False)
                features_dfs.append(features_within_bounds)
        features_within_bounds = pd.concat(features_dfs).drop_duplicates(subset=['GEOID'])

        intersections = features_within_bounds.intersection(other=within_union)
        features_within_bounds['geometry'] = intersections
        intersecting_mask = intersections.area/features_within_bounds.area >= area_threshold
        features_within = features_within_bounds[intersecting_mask]
        features_within = features_within.reset_index()
        return features_within

    def area(self, name: str = None, geoid: str = None, layer_name: str = '', cb: bool = True) -> Area:
        layer = self.get_layer(layer_name=layer_name)

        if not ((name is not None) ^ (geoid is not None)):
            raise ValueError('Must provide either a name or a geoid, but not both.')
        
        if name:
            area = layer.get_area_by_name(name=name, cb=cb)
        else:
            area = layer.get_area_by_geo_id(geoid=geoid, cb=cb)
        return area

    def area_multilayer(self, name: str = None, geoid: str = None, layer_names: List[str] = [], specific_layer: str = None, cb: bool = True) -> Area:
        if specific_layer:
            if specific_layer in layer_names:
                return self.area(name=name, geoid=geoid, layer_name=specific_layer)
            else:
                raise ValueError(f"layer must be one of: {layer_names}, not '{specific_layer}'")
        
        exceptions = []

        for layer_name in layer_names:
            if layer_name in self.available_layers:
                try:
                    self.area(name=name, geoid=geoid, layer_name=layer_name, cb=cb)
                except Exception as e:
                    exceptions.append(str(e))

        layer_name_string = ' '.join(f"'{layer_name}'")
        exception_string = f"Searched for '{name}' among the layers {layer_name_string}. No search was successful. The searches raised the following exceptions:\n\n"
        for e in exceptions:
            exception_string += f'{e}\n\n'

        raise Exception(exception_string)

    def region(self, region: str = None, geoid: str = None, cb: bool = True) -> Area:
        return self.area(name=region, geoid=geoid, layer_name='Census Regions', cb=cb)

    def division(self, division: str = None, geoid: str = None, cb: bool = True) -> Area:
        return self.area(name=division, geoid=geoid, layer_name='Census Divisions', cb=cb)

    def state(self, state: str = None, geoid: str = None, cb: bool = True) -> Area:
        return self.area(name=state, geoid=geoid, layer_name='States', cb=cb)

    def county(self, county: str = None, geoid: str = None, cb: bool = True) -> Area:
        return self.area(name=county, geoid=geoid, layer_name='Counties', cb=cb)

    def tract(self, geoid: str = None, cb: bool = True) -> Area:
        return self.area(geoid=geoid, layer_name='Census Tracts', cb=cb)

    def block_group(self, geoid: str = None, cb: bool = True) -> Area:
        return self.area(geoid=geoid, layer_name='Census Block Groups', cb=cb)

    def place(self, place: str = None, geoid: str = None, place_type: str = None, cb: bool = True) -> Area:
        return self.area_multilayer(name=place, geoid=geoid, layer_names=['Incorporated Places', 'Census Designated Places', 'County Subdivisions'], specific_layer=place_type, cb=cb)

    def MSA(self, MSA: str = None, geoid: str = None, M_type: str = None, cb: bool = True) -> Area:
        return self.area_multilayer(name=MSA, geoid=geoid, layer_names=['Metropolitan Statistical Areas', 'Micropolitan Statistical Areas'], specific_layer=M_type, cb=cb)

    def CSA(self, CSA: str = None, geoid: str = None, cb: bool = True) -> Area:
        return self.area(name=CSA, geoid=geoid, layer_name='Combined Statistical Areas', cb=cb)

    def congressional_district(self, congressional_district: str = None, geoid: str = None, cb: bool = True) -> Area:
        return self.area(name=congressional_district, geoid=geoid, layer_name='Congressional Districts', cb=cb)

    def ZCTA(self, geoid: str = None, cb: bool = True) -> Area:
        return self.area(geoid=geoid, layer_name='Census ZIP Code Tabulation Areas', cb=cb)