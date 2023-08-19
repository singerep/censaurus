from unittest import TestCase, main
from pandas import DataFrame
from geopandas import GeoDataFrame

from censaurus.tiger import Area, US_CARTOGRAPHIC, Layer, AreaCollection
from censaurus.api import TIGERClient


class TIGERTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.area_collection = AreaCollection()
        cls.tiger_client = TIGERClient()

    def test_area_inits(self):
        montgomery = Area.from_tiger(geo_id='24031', layer_id='82', layer_name='States', tiger_client=self.tiger_client)
        montgomery._set_attributes()
        self.assertEqual(montgomery.name, 'Montgomery County')

        US_CARTOGRAPHIC._set_attributes()
        self.assertEqual(US_CARTOGRAPHIC.name, 'United States (cartographic boundary)')

    def test_get_features(self):
        state_layer = self.area_collection.get_layer('States')
        states = state_layer.get_features()
        self.assertIsInstance(states, GeoDataFrame)
        self.assertEqual(len(states), 56)

    def test_area_search(self):
        la = self.area_collection.county('Los Angeles County, California')
        self.assertEqual(la.name, 'Los Angeles County')

        la = self.area_collection.county('Los Angeles County')
        self.assertEqual(la.name, 'Los Angeles County')

        la = self.area_collection.county('Los Angeles')
        self.assertEqual(la.name, 'Los Angeles County')

        md = self.area_collection.state('Maryland')
        self.assertEqual(md.name, 'Maryland')

        md = self.area_collection.state('MD')
        self.assertEqual(md.name, 'Maryland')

        md = self.area_collection.state(geoid=24)
        self.assertEqual(md.name, 'Maryland')

        ny01 = self.area_collection.congressional_district('NY-01')
        self.assertEqual(ny01.name, 'Congressional District 1')

        ny01 = self.area_collection.congressional_district('New York 1')
        self.assertEqual(ny01.name, 'Congressional District 1')


if __name__ == "__main__":
    main()