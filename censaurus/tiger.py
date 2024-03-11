from typing import Dict, List, Union, Iterable, Set, Callable, Tuple
from types import MethodType
from shapely import intersection
from shapely.geometry import shape, box
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from shapely.ops import transform
from pyproj import Transformer, CRS
from pandas import DataFrame, Series, concat
from geopandas import GeoDataFrame
from thefuzz import process
from thefuzz.fuzz import ratio
from shapely import union_all, union
from re import finditer, split, sub
from Levenshtein import distance, ratio
from scipy.optimize import linear_sum_assignment
from collections import defaultdict
from numpy import log
from json.decoder import JSONDecodeError
from fiona._err import CPLE_OpenFailedError
from fiona.errors import DriverError
from httpx import Response
# from matplotlib.pyplot import fill, axis
import matplotlib.pyplot as plt

from censaurus.api import TIGERClient, TIGERWebAPIError
from censaurus.constants import LAYER_RESULT_COUNT_MAP, FEATURE_ATTRIBUTE_MAP, ABBR_TO_FULL, FIPS_TO_FULL, ABBR_TO_FULL_REGEX

def shapely_to_esri_json(shape: Union[Polygon, MultiPolygon]):
    if isinstance(shape, Polygon):
        rings = [[list(p) for p in shape.reverse().exterior.coords]]
    else:
        rings = []
        for s in shape.geoms:
            rings.append([list(p) for p in s.reverse().exterior.coords])
    return str({'rings': rings})

def parse_name(name: str) -> str:
    """
    Parses the name of a geographic area. Replaces state abbreviations with full state
    names and removes any leading zeros.

    Parameters
    ==========
    name : :obj:`str`
        The name to parse.
    """
    for match in finditer(pattern=ABBR_TO_FULL_REGEX, string=name):
        state_abbr = match.string[match.start():match.end()]
        name = name.replace(state_abbr, ABBR_TO_FULL[state_abbr.upper()])

    name = sub(pattern='(?<!\d)0+(?=\d)', repl='', string=name)
    
    return name

def generate_detailed_name(feature: Series, layer_name: str) -> str:
    """
    Generates a detailed name of a geographic area. Parses the name and adds the state,
    if necessary.

    Parameters
    ==========
    feature : :class:`pandas.Series`
        A :class:`pandas.Series` representing the geographic area.
    layer_name : :obj:`str`
        The layer the geographic area comes from.
    """
    feature_attributes = feature.to_dict()

    feature_name = feature_attributes['NAME']
    detailed_name = parse_name(name=feature_name)

    if 'state' in feature_attributes and layer_name != 'States':
        state_full = FIPS_TO_FULL[feature_attributes['state']]
        detailed_name += f', {state_full}'
    
    return detailed_name

def tokenize_feature_name(feature_name: str) -> set:
    """
    Splits a detailed name of a geographic area into tokens.

    Parameters
    ==========
    feature_name : :obj:`str`
        The name to tokenize.
    """
    return set(split(pattern='\W+', string=feature_name))

def build_custom_scorer(count_map: Dict[str, int], N: int) -> Tuple[Callable[[str, str], float], Dict[str, float]]:
    """
    Constructs a custom scoring function for edit distance. The custom scorer is mostly
    based on Normalized Setwise Levenshtein Distance, proposed in 
    https://arxiv.org/pdf/1903.09238.pdf, with additions to account for word importance.

    Parameters
    ==========
    count_map : :obj:`dict` of :obj:`str`: :obj:`float`
        A dictionary that details the number of times each unique token in a set of 
        geographic area names appears.
    N : :obj:`int`
        The total number of tokens.
    """
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
    """
    An object representing a single geographic area.

    Attributes
    ==========
    name : :obj:`str`
        The name of the geographic area.
    layer_name : :obj:`str` or None
        The name of the layer the geographic area is derived from, if the area comes
        from TIGERWeb. Otherwise, this should be None.
    attributes : :obj:`dict` of :obj:`str`: :obj:`str`
        A dictionary detailing the attributes of the geographic area.
    geometry : :class:`shapely.Polygon` or :class:`shapely.MultiPolygon`
        The geometric boundary of the geographic area.
    """
    def __init__(self) -> None:
        self.name : str = None
        self._attributes_are_set = False
        self.tiger_client : str = None
        self.layer_name : str = None
        self.attributes : Dict[str, str] = {}
        self.geometry : Union[Polygon, MultiPolygon] = None

    def __repr__(self) -> str:
        if self._attributes_are_set is False:
            self._set_attributes()
        area_str = f'Name: {self.name}\nAttributes:\n'
        if len(self.attributes) == 0:
            area_str += '  None'
        for attr, val in self.attributes.items():
            area_str += f'  - {attr}: {val}\n'
        return area_str

    def _set_attributes(self) -> None:
        ...

    def plot(self, **kwargs) -> None:
        """
        Generates a plot of the geographic area using ``matplotlib``.

        Parameters
        ==========
        **kwargs
            Any additional plotting parameters to pass to ``matplotlib`` when calling
            ``matplotlib.pyplot.fill()``.
        """
        if self._attributes_are_set is False:
            self._set_attributes()
        if type(self.geometry) == Polygon:
            x, y = self.geometry.exterior.xy
            plt.fill(x, y, **kwargs)
        elif type(self.geometry) == MultiPolygon:
            for g in self.geometry.geoms:
                x, y = g.exterior.xy
                plt.fill(x, y, **kwargs)
        plt.axis('equal')

    def clip_to_cb(self) -> None:
        """
        Intersects the geometric boundary of the geographic area with the cartographic
        boundary of the United States.
        """
        if self.geometry is None:
            self._set_attributes()
        if US_CARTOGRAPHIC.geometry is None:
            US_CARTOGRAPHIC._set_attributes()
        self.geometry = intersection(self.geometry, US_CARTOGRAPHIC.geometry)

    def remove_water(self, area_threshold: float = 0.1, keep_internal: bool = False):
        water_geom = _water_within_geometry(geometry=self.geometry, area_threshold=area_threshold, keep_internal=keep_internal)

        if water_geom is not None:
            self.geometry = self.geometry.difference(water_geom)

    @classmethod
    def from_tiger_geo_id(cls, geo_id: str, layer_id: int, layer_name: str, tiger_client: TIGERClient, clip_to_cb: bool = True) -> 'Area':
        """
        Constructs a :class:`.Area` object from TIGERWeb given a specific geographic
        identifier.

        Parameters
        ==========
        geo_id : :obj:`str`
            The geographic identifier of the area.
        layer_id : :obj:`int`
            The identifier of the layer the area is derived from.
        layer_name : :obj:`str`
            The name of the layer the area is derived from.
        tiger_client : :class:`.TIGERClient`
            A client to use to interface with TIGERWeb.
        clip_to_cb : :obj:`bool` = True
            Determines whether the geometric boundary of the area should be intersected
            with the cartographic boundary of the United States.
        """
        def _set_attributes(self: Area):
            if self._attributes_are_set is True:
                return
            
            params = {
                'where': f"GEOID='{geo_id}'",
                'outFields': '*',
                'returnGeometry': 'true',
                'geometryPrecision': '2',
                'outSR': '3857'
            }
            area_resp = tiger_client.get_sync(f'{layer_id}/query', params=params, return_type='geojson')
            feature = area_resp.json()['features'][0]

            for attr, val in feature['properties'].items():
                if attr == 'NAME' or attr == 'BASENAME':
                    if attr == 'NAME':
                        self.name = val
                    continue
                
                attr = FEATURE_ATTRIBUTE_MAP.get(attr, attr)
                self.attributes[attr] = val

            self.tiger_client = tiger_client
            self.layer_name = layer_name

            self.geometry = shape(feature['geometry'])
            if clip_to_cb is True:
                self.clip_to_cb()
            self._attributes_are_set = True

        area = cls()
        area._set_attributes = MethodType(_set_attributes, area)

        return area

    @classmethod
    def from_url(cls, name: str, url: str, geo_col: str = 'geometry', clip_to_cb: bool = True) -> 'Area':
        """
        Constructs a :class:`.Area` object from a URL.

        Parameters
        ==========
        name : :obj:`str`
            The name of the geographic area.
        url : :obj:`str`
            The URL to fetch the geographic area from.
        geo_col : :obj:`str`
            The column within the file the ``url`` points to that details the geometry
            of the area.
        clip_to_cb : :obj:`bool` = True
            Determines whether the geometric boundary of the area should be intersected
            with the cartographic boundary of the United States.
        """
        return cls._from_file_or_url(name=name, path=url, kind='URL', geo_col=geo_col, clip_to_cb=clip_to_cb)

    @classmethod
    def from_file(cls, name: str, filename: str, geo_col: str = 'geometry', clip_to_cb: bool = True) -> 'Area':
        """
        Constructs a :class:`.Area` object from a filename.

        Parameters
        ==========
        name : :obj:`str`
            The name of the geographic area.
        filename : :obj:`str`
            The filename to read the geographic area from.
        geo_col : :obj:`str`
            The column within the file the ``filename`` points to that details the geometry
            of the area.
        clip_to_cb : :obj:`bool` = True
            Determines whether the geometric boundary of the area should be intersected
            with the cartographic boundary of the United States.
        """
        return cls._from_file_or_url(name=name, path=filename, kind='filename', geo_col=geo_col, clip_to_cb=clip_to_cb)

    @classmethod
    def _from_file_or_url(cls, name: str, path: str, kind: str, geo_col: str = 'geometry', clip_to_cb: bool = True) -> 'Area':
        def _set_attributes(self: Area):
            if self._attributes_are_set is True:
                return
            
            try:
                gdf = GeoDataFrame.from_file(path)
            except (CPLE_OpenFailedError, DriverError):
                raise ValueError(f"The {kind} you provided must point to a file of any file format recognized by 'fiona' (see http://fiona.readthedocs.io/en/latest/manual.html).")

            if geo_col not in gdf:
                raise ValueError(f"The {kind} you provided must point to a file with the geometry column '{geo_col}'. The columns of the file your URL pointed to were: {gdf.columns}")

            self.name = name

            if len(gdf) == 0:
                self.geometry = gdf[geo_col].to_crs('EPSG:3857').values[0]
            else:
                geometries = gdf[geo_col].to_crs('EPSG:3857').values
                self.geometry = union_all(geometries)
            
            if clip_to_cb is True:
                self.clip_to_cb()
            self.attributes = gdf.to_dict(orient='index')[0]
            del self.attributes[geo_col]
            self._attributes_are_set = True
        
        area = cls()
        area._set_attributes = MethodType(_set_attributes, area)
        return area


US_CARTOGRAPHIC = Area.from_url(name='United States (cartographic boundary)', url='https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_nation_5m.zip', clip_to_cb=False)
# US_LANDMASS = Area.from_url(name='United States (landmass boundary)', url='https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/USLandmass/MapServer/0/query?where=1%3D1&text=&objectIds=&time=&timeRelation=esriTimeRelationOverlaps&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=OBJECTID&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=6&outSR=3857&havingClause=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&returnExtentOnly=false&sqlFormat=none&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=geojson', clip_to_cb=False)

class Layer:
    """
    An object representing a layer of a TIGERWeb MapService.

    Parameters
    ==========
    info : :obj:`dict` of :obj:`str`: :obj:`str`
        A dictionary detailing the attributes of the layer.
    tiger_client : :class:`.TIGERClient`
        A client to use to interface with TIGERWeb.

    Attributes
    ==========
    name : :obj:`str`
        The name of the TIGERWeb MapService layer.
    id : :obj:`str`
        The identifier of the TIGERWeb MapService layer.
    fields : :obj:`list` of :obj:`str`
        The available fields to use when querying this layer.
    """
    def __init__(self, info: Dict[str, str], tiger_client: TIGERClient) -> None:
        self.name = info['name']
        self.id = info['id']
        self.fields = [f['name'] for f in info.get('fields')]
        self.critical_fields = [f for f in self.fields if f in FEATURE_ATTRIBUTE_MAP or f in ['OBJECTID', 'GEOID', 'NAME']]
        self.geographic_fields = [f for f in self.fields if f in FEATURE_ATTRIBUTE_MAP]
        self.max_record_count = info['maxRecordCount']

        self.tiger_client = tiger_client

    def __repr__(self) -> str:
        return f'MapService Layer ({self.name})'

    def _paginate_through_features(self, params: Dict, result_record_count: int, feature_count: int):
        tries = 0
        while True:
            try:
                tries += 1
                retrieved_all = False
                all_feature_responses = []

                result_offset = 0

                while retrieved_all is False:
                    params_list = []
                    for _ in range(1 + (feature_count//result_record_count)):
                        params = params.copy()
                        params['resultRecordCount'] = result_record_count
                        params['resultOffset'] = result_offset
                        params_list.append(params)
                        
                        result_offset += result_record_count

                    urls_list = [f'{self.id}/query']*len(params_list)
                    url_params_list = zip(urls_list, params_list)
                    
                    features_responses = self.tiger_client.post_many_sync(url_data_list=url_params_list, return_type='geojson')
                    all_feature_responses.extend(features_responses)

                    if any([r.json().get('exceededTransferLimit', False) == False for r in features_responses]):
                        retrieved_all = True

                return all_feature_responses
            except TIGERWebAPIError:
                if tries <= 2:
                    result_record_count = result_record_count // 2
                else:
                    raise TIGERWebAPIError(None, 'There was a problem generating TIGERWeb API calls. Please try again or request a smaller geography set.')
            except JSONDecodeError:
                raise TIGERWebAPIError(None, 'There was a problem decoding the result of your TIGER API call. Please try again or request a different geography.')

    def _convert_feature_responses_to_gdf(self, feature_responses: List[Response]) -> GeoDataFrame:
        gdfs = []
        for features_resp in feature_responses:
            features = features_resp.json()['features']
            gdf = GeoDataFrame.from_features(features=features)
            gdfs.append(gdf)
        
        gdf = GeoDataFrame(concat(gdfs))
        gdf = gdf.reset_index()

        return gdf

    def _get_feature_attributes(self, within_geometry: Union[Polygon, MultiPolygon] = None, where_condition: str = '1=1', out_fields: str = '*', spatial_rel: str = 'esriSpatialRelContains', feature_count: int = None) -> GeoDataFrame:
        params = {
            'where': where_condition,
            'outFields': out_fields,
            'returnGeometry': 'false'
        }
        if within_geometry:
            params.update({
                'geometry': shapely_to_esri_json(within_geometry),
                'geometryType': 'esriGeometryPolygon',
                'inSR': '3857',
                'spatialRel': spatial_rel
            })

        if feature_count is None:
            canary_params = params.copy()
            canary_params['returnCountOnly'] = 'true'
            feature_count = self.tiger_client.post_sync(url=f'{self.id}/query', data=canary_params).json()['count']

        feature_responses = self._paginate_through_features(params=params, result_record_count=self.max_record_count, feature_count=feature_count)
        gdf = self._convert_feature_responses_to_gdf(feature_responses)
        return gdf

    def _get_feature_geometry(self, within_geometry: Union[Polygon, MultiPolygon] = None, where_condition: str = '1=1', out_fields: str = 'GEOID,OBJECTID', area_threshold: float = None, feature_count: int = None, object_ids: List[int] = None) -> GeoDataFrame:
        params = {
            'where': where_condition,
            'outFields': out_fields,
            'returnGeometry': 'true',
            'geometryPrecision': '2',
            'outSR': '3857'
        }

        if within_geometry and not object_ids:
            params.update({
                'geometry': shapely_to_esri_json(within_geometry),
                'geometryType': 'esriGeometryPolygon',
                'inSR': '3857',
                'spatialRel': 'esriSpatialRelIntersects'
            })

        if object_ids:
            params.update({'objectIds': ','.join(str(i) for i in object_ids)})

        if feature_count is None:
            if object_ids is None:
                canary_params = params.copy()
                canary_params['returnCountOnly'] = 'true'
                feature_count = self.tiger_client.post_sync(url=f'{self.id}/query', data=canary_params).json()['count']
            else:
                feature_count = len(object_ids)

        if self.name in LAYER_RESULT_COUNT_MAP:
            result_record_count = LAYER_RESULT_COUNT_MAP[self.name]
        else:
            result_record_count = 100

        feature_responses = self._paginate_through_features(params=params, result_record_count=result_record_count, feature_count=feature_count)

        gdf = self._convert_feature_responses_to_gdf(feature_responses)

        if len(gdf) > 0:
            if within_geometry and area_threshold is not None:
                intersections = gdf.intersection(other=within_geometry)

                intersecting_mask = intersections.area/gdf.area >= area_threshold
                gdf['geometry'] = intersections

                gdf = gdf[intersecting_mask]
            
            gdf.set_crs('EPSG:3857', inplace=True)
            gdf = gdf.reset_index()

        return gdf

    def get_features(self, within_geometry: Union[Polygon, MultiPolygon] = None, area_threshold: float = None, where_condition: str = '1=1', out_fields: Union[str, List[str]] = None, return_attributes: bool = True, return_geometry: bool = False) -> GeoDataFrame:
        """
        Get a set of features in this layer.

        Parameters
        ==========
        within_geometry : :obj:`shapely.Polygon` or :obj:`shapely.MultiPolygon` = None
            The region used to subset features in the layer. Points should be in CRS 
            EPSG:3857. If None, no geographic filters are applied.
        area_threshold : :obj:`float` = None
            Controls what proportion of the area of each feature needs to be inside
            the ``within_geometry`` for it to be included. If ``within_geometry`` is
            ``None``, this parameter has no effect.
        where_condition : :obj:`str` = "1=1"
            The SQL condition used to filter results from the layer. Cannot be empty;
            the default, "1=1", will always be ``True``, and so it returns all features.
            To see what attributes can be used in this condition, check out
            :attr:`.Layer.fields`.
        out_fields : :obj:`str` or :obj:`List` of :obj:`str`  = None
            Controls what parameters are returned for each feature. If None, then
            only fields which are geographic specifiers, plus NAME, GEOID, and OBJECTID,
            are returned.
        return_geometry : :obj:`bool` = False
            Determines whether or not the attributes of each feature are included in the
            returned :class:`geopandas.GeoDataFrame`.
        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each feature is included in the
            returned :class:`geopandas.GeoDataFrame`.
        """
        if out_fields is None:
            out_fields = self.critical_fields

        if isinstance(out_fields, list):
            out_fields = ','.join(out_fields)

        if return_attributes:
            features = self._get_feature_attributes(within_geometry=within_geometry, where_condition=where_condition, out_fields=out_fields, spatial_rel='esriSpatialRelIntersects')
            if area_threshold is not None: # TODO: add notes about this
                geometries = self._get_feature_geometry(within_geometry=within_geometry, where_condition=where_condition, out_fields='GEOID', area_threshold=area_threshold, object_ids=features['OBJECTID'].to_list())
                features = GeoDataFrame(features.drop(labels=['geometry'], axis=1).merge(geometries, on='GEOID', how='inner'))
            if return_geometry is False:
                features = DataFrame(features.drop(labels=['geometry'], axis=1))
        else:
            features = self._get_feature_geometry(within_geometry=within_geometry, where_condition=where_condition, out_fields=out_fields, area_threshold=area_threshold)

        features = features.rename(columns=FEATURE_ATTRIBUTE_MAP)

        return features

    def get_area_by_geo_id(self, geoid: str, cb: bool = True) -> Area:
        """
        Searches the layer for a feature from a geographic identifier.

        geoid : :obj:`str`
            The identifier to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting feature will be
            intersected with the cartographic boundary of the United States.
        """
        geoid = str(geoid)
        features = self.get_features(out_fields='GEOID')
        geoids = features['GEOID'].values
        if geoid in geoids:
            print(f"successfully matched GEOID = {geoid} in layer '{self.name}'")
            area = Area.from_tiger_geo_id(geo_id=geoid, layer_id=self.id, layer_name=self.name, tiger_client=self.tiger_client, clip_to_cb=cb)
            area._set_attributes()
            return area
        
        exception_string = f"The GEOID '{geoid}' does not have any matches within the layer '{self.name}'. Please ensure you are searching at the correct geographic level. For reference, here are some examples of GEOIDs within this layer.\n"
        for geoid in geoids[:5]:
            exception_string += f'  - {geoid}\n'
        raise Exception(exception_string)

    def get_area_by_name(self, name: str, cb: bool = True) -> Area:
        """
        Searches the layer for a feature from a name. Please be as detailed as possible
        when searching for a name. For example, you should use 
        ``Los Angeles, California`` instead of ``LA`` or even ``Los Angeles``.
        Name matching is done with a custom string distance function partially based
        on Normalized Setwise Levenshtein Distance, as defined in 
        https://arxiv.org/pdf/1903.09238.pdf.

        name : :obj:`str`
            The name to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting feature will be
            intersected with the cartographic boundary of the United States.
        """
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
            area = Area.from_tiger_geo_id(geo_id=geoid, layer_id=self.id, layer_name=self.name, tiger_client=self.tiger_client, clip_to_cb=cb)
            area._set_attributes()
            return area
        
        elif best_matches[0][1] > 0.95 and best_matches[0][1] - best_matches[1][1] > 0.05:
            geoid = best_matches[0][2]
            area_name = best_matches[0][0]
            print(f"successfully matched '{name}' to '{area_name}' (GEOID = {geoid}) in layer '{self.name}'")
            area = Area.from_tiger_geo_id(geo_id=geoid, layer_id=self.id, layer_name=self.name, tiger_client=self.tiger_client, clip_to_cb=cb)
            area._set_attributes()
            return area
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
                print(f"successfully matched '{name}' to '{area_name}' (GEOID = {geoid}) in layer '{self.name}'")
                area = Area.from_tiger_geo_id(geo_id=geoid, layer_id=self.id, layer_name=self.name, tiger_client=self.tiger_client, clip_to_cb=cb)
                area._set_attributes()
                return area

        exception_string = f"The name '{name}' is ambiguous. Is there a typo? Could you be more specific? Did you mean any of the following (in no particular order)?\n"
        for match in best_matches[:10]:
            exception_string += f'  - {match[0]} (GEOID = {match[2]})\n'
        raise Exception(exception_string)


class AreaCollection:
    """
    An object that represents a collection of geographic areas.

    Parameters
    ==========
    map_service :obj:`str` = 'tigerWMS_Current'
        The TIGERWeb MapService to use as the basis for this collection. Defaults to the
        current map service.

    Attributes
    ==========
    tiger_client : :class:`.TIGERClient`
        A client to use to interface with TIGERWeb.
    available_layers : :obj:`dict` of :obj:`str`, :class:`.Layer`
        A dictionary detailing the available layers for this collection, based on the
        ``map_service`` used.
    """
    def __init__(self, map_service: str = 'tigerWMS_Current') -> None:
        self.map_service = map_service
        self.tiger_client = TIGERClient(map_service=map_service)
        self.available_layers = self._find_available_layers()

    def _find_available_layers(self) -> Dict[str, Layer]:
        available_layers = {}
        layers_response = self.tiger_client.get_sync(f'layers')
        layers = layers_response.json()['layers']
        for l in layers:
            if 'Labels' not in l['name']:
                layer = Layer(l, tiger_client=self.tiger_client)
                available_layers[layer.name] = layer
        return available_layers

    def get_layer(self, layer_name: str) -> Layer:
        """
        Searches the available layers by name.

        Parameters
        ==========
        layer_name :obj:`str`
            The name to search for.
        """
        token_sets = [tokenize_feature_name(l) for l in self.available_layers.keys()]
        N = len(token_sets)
        count_map = defaultdict(int)
        for token_set in token_sets:
            for token in token_set:
                count_map[token.lower()] += 1

        custom_scorer, idf_map = build_custom_scorer(count_map=count_map, N=N)
        layer_token_set_dict = dict(zip(self.available_layers.keys(), self.available_layers.keys()))

        match, score, tokens = process.extractOne(query=layer_name, choices=layer_token_set_dict, scorer=custom_scorer)
        if score >= 0.80:
            return self.available_layers[match]
        else:
            raise ValueError(f"The layer '{layer_name}' is not available for this dataset. To see the available layers, see AreaCollection.available_layers.")

    def get_features_within(self, within: Union[Area, List[Area]], layer_name: Union[str, List[str]], area_threshold: float, return_attributes: bool, return_geometry: bool):
        """
        Gets the features within a geographic area (or areas) and a specific layer.

        within : :class:`.Area` or :obj:`list` of :class:`.Area` = None
            A :class:`censaurus.Area` object or a :obj:`list` of :class:`censaurus.Area`
            objects. Only geographic areas whose geometries lay within the bounds of the
            ``within`` area (or areas) will be included. Note that what it means for an 
            area to be "within" depends on the ``area_threshold`` parameter.
        layer_name : :obj:`str`
            The name of the TIGERWeb MapService layer to query.
        area_threshold : :obj:`float`
            Only geographic areas where (``total area of the geography`` intersected 
            with the ``within`` area (or areas) / ``total area of the geography``) is greater
            than ``area_threshold`` will be included. The default of 0.01 ensures that
            geographic areas that only intersect with the ``within`` area (or areas)
            on the boundary will not be included (boundary intersections have zero 
            area).
        return_geometry : :obj:`bool` = False
            Determines whether or not the attributes of each feature are included in the
            returned :class:`geopandas.GeoDataFrame`.
        return_geometry : :obj:`bool` = False
            Determines whether or not the geometry of each feature is included in the
            returned :class:`geopandas.GeoDataFrame`.
        """
        if isinstance(within, Area):
            within = [within]

        if isinstance(within, List):
            for area in within:
                if area._attributes_are_set is False:
                    area._set_attributes()
            within_union = union_all(geometries=[a.geometry for a in within])
        else:
            within_union = None

        if isinstance(layer_name, str):
            layer_name = [layer_name]

        features_dfs = []
        for name in layer_name:
            layer = self.get_layer(layer_name=name)
            features_within_bounds = layer.get_features(within_geometry=within_union, area_threshold=area_threshold, return_attributes=return_attributes, return_geometry=return_geometry)
            features_dfs.append(features_within_bounds)
        features_within_bounds = concat(features_dfs).drop_duplicates(subset=['GEOID'])

        return features_within_bounds

    def area(self, name: str = None, geoid: str = None, layer_name: str = '', cb: bool = True) -> Area:
        """
        Searches a layer for an area with a given name or identifier. Note that you 
        should only supply a name or an identifier, but not both.

        Parameters
        ==========
        name : :obj:`str` = None
            A name to search for.
        geoid : :obj:`str` = None
            A geographic identifier to search for.
        layer_name : :obj:`str` = ''
            The layer to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting feature will be
            intersected with the cartographic boundary of the United States. 
        """
        layer = self.get_layer(layer_name=layer_name)

        if not ((name is not None) ^ (geoid is not None)):
            raise ValueError('Must provide either a name or a geoid, but not both.')
        
        if name:
            area = layer.get_area_by_name(name=name, cb=cb)
        else:
            area = layer.get_area_by_geo_id(geoid=geoid, cb=cb)
        return area

    def area_multilayer(self, name: str = None, geoid: str = None, layer_names: List[str] = [], specific_layer: str = None, cb: bool = True) -> Area:
        """
        Searches a layer (or layers) for an area with a given name or identifier. Note
        that you should only supply a name or an identifier, but not both.

        Parameters
        ==========
        name : :obj:`str` = None
            A name to search for.
        geoid : :obj:`str` = None
            A geographic identifier to search for.
        layer_names : :obj:`list` of :obj:`str` = []
            A list of layers to search for.
        specific_layer : :obj:`str` = None
            A specific layers to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting feature will be
            intersected with the cartographic boundary of the United States.
        """
        if specific_layer:
            if specific_layer in layer_names:
                return self.area(name=name, geoid=geoid, layer_name=specific_layer)
            else:
                raise ValueError(f"layer must be one of: {layer_names}, not '{specific_layer}'")
        
        exceptions = []

        for layer_name in layer_names:
            if layer_name in self.available_layers:
                try:
                    return self.area(name=name, geoid=geoid, layer_name=layer_name, cb=cb)
                except Exception as e:
                    exceptions.append(str(e))

        exception_string = f"Searched for '{name}' among the layers {layer_names}. No search was successful. The searches raised the following exceptions:\n\n"
        for e in exceptions:
            exception_string += f'{e}\n\n'

        raise Exception(exception_string)

    def region(self, region: str = None, geoid: str = None, cb: bool = True) -> Area:
        """
        Searches the Census Regions layer for a specific region. See 
        `here <https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf>`_
        for an overview of Census regions. Can search by name or geographic identifier.
        Note that you should only supply a region name or an identifier, but not both.
        Available region names are:

           + Northeast Region
           + South Region
           + Midwest Region
           + West Region

        Parameters
        ==========
        region : :obj:`str` = None
            The name of the region to search for.
        geoid : :obj:`str` = None
            The identifier of the region to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting region will be
            intersected with the cartographic boundary of the United States. 
        """
        return self.area(name=region, geoid=geoid, layer_name='Census Regions', cb=cb)

    def division(self, division: str = None, geoid: str = None, cb: bool = True) -> Area:
        """
        Searches the Census Divisions layer for a specific division. See 
        `here <https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf>`_
        for an overview of Census divisions. Can search by name or geographic 
        identifier. Note that you should only supply a region name or an identifier, but
        not both. Available division names are:

           + New England Division
           + Middle Atlantic Division
           + South Atlantic Division
           + East South Central Division
           + West South Central Division
           + East North Central Division
           + West North Central Division
           + Mountain Division
           + Pacific Division

        Parameters
        ==========
        division : :obj:`str` = None
            The name of the division to search for.
        geoid : :obj:`str` = None
            The identifier of the region to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting division will be
            intersected with the cartographic boundary of the United States. 
        """
        return self.area(name=division, geoid=geoid, layer_name='Census Divisions', cb=cb)

    def state(self, state: str = None, geoid: str = None, cb: bool = True) -> Area:
        """
        Searches the States layer for a specific state. Can search by name or geographic 
        identifier. Note that you should only supply a state name or an identifier, but
        not both. State names can be abbreviations (AL, AK, AZ, etc.) or full names
        (Alabama, Alaska, Arizona, etc.).

        Parameters
        ==========
        state : :obj:`str` = None
            The name of the state to search for.
        geoid : :obj:`str` = None
            The identifier of the state to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting state will be
            intersected with the cartographic boundary of the United States. 
        """
        return self.area(name=state, geoid=geoid, layer_name='States', cb=cb)

    def county(self, county: str = None, geoid: str = None, cb: bool = True) -> Area:
        """
        Searches the Counties layer for a specific county. Can search by name or 
        geographic identifier. Note that you should only supply a county name or an 
        identifier, but not both. County names should ideally be as specific as possible
        to ensure correct matches. Some examples include:

           + ``Los Angeles, California``
           + ``Montgomery County, Maryland``
           + ``Wayne County, Michigan``

        Parameters
        ==========
        county : :obj:`str` = None
            The name of the county to search for.
        geoid : :obj:`str` = None
            The identifier of the county to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting county will be
            intersected with the cartographic boundary of the United States. 
        """
        return self.area(name=county, geoid=geoid, layer_name='Counties', cb=cb)

    def tract(self, geoid: str = None, cb: bool = True) -> Area:
        """
        Searches the Census Tracts layer for a specific tract.

        Parameters
        ==========
        geoid : :obj:`str` = None
            The identifier of the tract to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting tract will be
            intersected with the cartographic boundary of the United States. 
        """
        return self.area(geoid=geoid, layer_name='Census Tracts', cb=cb)

    def block_group(self, geoid: str = None, cb: bool = True) -> Area:
        """
        Searches the Census Block Groups layer for a specific block group.

        Parameters
        ==========
        geoid : :obj:`str` = None
            The identifier of the block group to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting block group will be
            intersected with the cartographic boundary of the United States. 
        """
        return self.area(geoid=geoid, layer_name='Census Block Groups', cb=cb)

    def block(self, geoid: str = None, cb: bool = True) -> Area:
        """
        Searches the Census Blocks layer for a specific block.

        Parameters
        ==========
        geoid : :obj:`str` = None
            The identifier of the block to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting block will be
            intersected with the cartographic boundary of the United States. 
        """
        return self.area(geoid=geoid, layer_name='Census Blocks', cb=cb)

    def place(self, place: str = None, geoid: str = None, place_type: str = None, cb: bool = True) -> Area:
        """
        Searches the place layers for a specific Census place. Can search by name or 
        geographic identifier. Note that you should only supply a place name or an 
        identifier, but not both. Place names should ideally be as specific as possible
        to ensure correct matches. Some examples include:

           + ``Chicago, Illinois``
           + ``Houston, Texas``
           + ``Seneca Falls, New York``

        Parameters
        ==========
        place : :obj:`str` = None
            The name of the place to search for.
        geoid : :obj:`str` = None
            The identifier of the place to search for.
        place_type : :obj:`str` = None
            The type of the Census place you are search for. Should be either 
            ``Census Designated Places`` or ``Incorporated Places``. Not specifying this
            value means that both layers will be searched, which can take longer.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting place will be
            intersected with the cartographic boundary of the United States. 
        """
        return self.area_multilayer(name=place, geoid=geoid, layer_names=['Incorporated Places', 'Census Designated Places', 'County Subdivisions'], specific_layer=place_type, cb=cb)

    def MSA(self, MSA: str = None, geoid: str = None, M_type: str = None, cb: bool = True) -> Area:
        """
        Searches the MSA layers for a specific MSA. Can search by name or 
        geographic identifier. Note that you should only supply a MSA name or an 
        identifier, but not both. MSA names should ideally be as specific as possible
        to ensure correct matches. Some examples include:

           + ``Knoxville, Tennessee Metro Area``
           + ``Baltimore-Columbia-Towson, Maryland Metro Area``
           + ``Ithaca, New York Metro Area``

        Parameters
        ==========
        MSA : :obj:`str` = None
            The name of the MSA to search for.
        geoid : :obj:`str` = None
            The identifier of the MSA to search for.
        M_type : :obj:`str` = None
            The type of the MSA you are search for. Should be either 
            ``Metropolitan Statistical Area`` or ``Micropolitan Statistical Area``. 
            Not specifying this value means that both layers will be searched, which 
            can take longer.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting MSA will be
            intersected with the cartographic boundary of the United States. 
        """
        return self.area_multilayer(name=MSA, geoid=geoid, layer_names=['Metropolitan Statistical Areas', 'Micropolitan Statistical Areas'], specific_layer=M_type, cb=cb)

    def CSA(self, CSA: str = None, geoid: str = None, cb: bool = True) -> Area:
        """
        Searches the Combined Statistical Area layer for a specific CSA. Can search by 
        name or geographic identifier. Note that you should only supply a CSA name or an 
        identifier, but not both. CSA names should ideally be as specific as possible
        to ensure correct matches. Some examples include:

           + ``Phoenix-Mesa, Arizona CSA``
           + ``Minneapolis-St. Paul, Minnesota-Wisconsin CSA``
           + ``Dallas-Fort Worth, TX-OK Combined Statistical Area``

        Parameters
        ==========
        CSA : :obj:`str` = None
            The name of the CSA to search for.
        geoid : :obj:`str` = None
            The identifier of the CSAs to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting CSA will be
            intersected with the cartographic boundary of the United States. 
        """
        return self.area(name=CSA, geoid=geoid, layer_name='Combined Statistical Areas', cb=cb)

    def congressional_district(self, congressional_district: str = None, geoid: str = None, cb: bool = True) -> Area:
        """
        Searches the Congressional Districts layer for a specific district. Can search 
        by name or geographic identifier. Note that you should only supply a district
        name or an identifier, but not both. District names should ideally be as 
        specific as possible to ensure correct matches.

           + ``Ohio District 1``
           + ``UT-01``
           + ``New York 1``

        Parameters
        ==========
        congressional_district : :obj:`str` = None
            The name of the congressional district to search for.
        geoid : :obj:`str` = None
            The identifier of the congressional district to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting congressional
            district will be intersected with the cartographic boundary of the United
            States. 
        """
        return self.area(name=congressional_district, geoid=geoid, layer_name='Congressional Districts', cb=cb)

    def ZCTA(self, geoid: str = None, cb: bool = True) -> Area:
        """
        Searches the Census ZIP Code Tabulation Areas layer for a specific ZCTA.

        Parameters
        ==========
        geoid : :obj:`str` = None
            The identifier of the ZCTA to search for.
        cb : :obj:`bool` = True
            Determines whether or not the geometry of the resulting ZCTA will be
            intersected with the cartographic boundary of the United States. 
        """
        return self.area(geoid=geoid, layer_name='Census ZIP Code Tabulation Areas', cb=cb)


def _water_within_geometry(geometry: Union[Polygon, MultiPolygon], area_threshold: float, keep_internal: bool = False) -> Union[Polygon, MultiPolygon]:
    projection = Transformer.from_crs(CRS('EPSG:3857'), CRS('EPSG:9822'), always_xy=True).transform
    geometry_equal_area = transform(projection, geometry)
    minimum_area = int(geometry_equal_area.area*area_threshold)
    
    where_condition = f"AREAWATER > {minimum_area}"

    water_collection = AreaCollection(map_service='Hydro')
    areal_water = water_collection.get_layer('Areal Hydrography')

    if keep_internal is False:
        # internal_water_types = ['Inlt', 'Sea', 'Strait', 'Lk', 'Reservoir', 'Ocean', 'Bay', 'Hbr', 'Chnnl', 'Lks', 'Waterway'] # all water types: {'Flowage', 'Run', 'Strm', 'Inlt', None, 'Br', 'Tank', 'Brk', 'Fls', 'Byu', 'Sea', 'Strait', 'Slough', 'Cv', 'Psge', 'Outlet', 'Lk', 'Reservoir', 'Ocean', 'Bay', 'Millpond', 'Riv', 'Aqueduct', 'Prong', 'Frk', 'Ditch', 'Pond', 'Marsh', 'Bog', 'Hbr', 'Chnnl', 'Holw', 'Lks', 'Crk', 'Lagoon', 'Spg', 'Swamp', 'Waterway', 'Cnl', 'Draw', 'Ponds'}
        internal_water_types = ['Flowage', 'Run', 'Strm', 'Br', 'Tank', 'Brk', 'Fls', 'Byu', 'Slough', 'Cv', 'Psge', 'Outlet', 'Millpond', 'Aqueduct', 'Prong', 'Frk', 'Ditch', 'Pond', 'Marsh', 'Bog', 'Holw', 'Crk', 'Lagoon', 'Spg', 'Swamp', 'Cnl', 'Draw', 'Ponds']
        where_condition += ' AND (SUFTYPEABRV NOT IN (' + ','.join([f"'{t}'" for t in internal_water_types]) + ') OR SUFTYPEABRV IS NULL)'

    water = areal_water.get_features(within_geometry=geometry, where_condition=where_condition, out_fields='OBJECTID,SUFTYPEABRV', return_attributes=False, return_geometry=True)

    if len(water) > 0:
        water_geoms = water['geometry'].values
        water_union = union_all(water_geoms)

        if keep_internal is False:
            bounding_polygon = box(*geometry.bounds)
            difference = bounding_polygon.difference(geometry)

            if isinstance(water_union, Polygon):
                water_bodies = [water_union]
            else:
                water_bodies = water_union.geoms

            external_water_bodies = []
            for b in water_bodies:
                if b.distance(difference) <= 10:
                    external_water_bodies.append(b)
            water_geoms = external_water_bodies

        return union_all(water_geoms)
    else:
        return None 