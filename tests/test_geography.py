from unittest import TestCase, main
from pandas import DataFrame

from censaurus.dataset import ACS1
from censaurus.geography import Geography, GeographyCollection, UnknownGeography


class GeographyTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset = ACS1()

    def test_existing_geography(self):
        self.assertIsInstance(self.dataset.geographies, GeographyCollection)
        counties_geography = self.dataset.geographies.get(level='050')
        self.assertIn('state', counties_geography.requires)
        self.assertIsInstance(counties_geography, Geography)

    def test_nonexisting_geography(self):
        with self.assertRaises(UnknownGeography) as context:
            self.dataset.geographies.get(level='000')

    def test_collection_length(self):
        self.assertEqual(len(self.dataset.geographies), 23)

    def test_collection_outputs(self):
        self.assertIsInstance(self.dataset.geographies.to_df(), DataFrame)
        self.assertIsInstance(self.dataset.geographies.to_list(), list)


if __name__ == "__main__":
    main()