from unittest import TestCase, main
from pandas import DataFrame
from geopandas import GeoDataFrame

from censaurus.dataset import DatasetExplorer, ACS, ACS1, ACS3, ACS5, ACSSupplemental, ACSFlows, ACSLanguage, PUMS, CPS, Decennial, DecennialPL, DecennialSF1, DecennialSF2, Economic, EconomicKeyStatistics, Estimates, Projections
from censaurus.geography import UnknownGeography


class DatasetTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.all_dataset_classes = [ACS, ACS1, ACS3, ACS5, ACSSupplemental, ACSFlows, ACSLanguage, PUMS, CPS, Decennial, DecennialPL, DecennialSF1, DecennialSF2, Economic, EconomicKeyStatistics, Estimates, Projections]
        cls.acs1 = ACS1()

    def test_explorer(self):
        explorer = DatasetExplorer()
        self.assertEqual(len(explorer.filter_by_term(term=['american community survey', '5']).filter_by_year(start_year=2010, end_year=2020).to_df()), 42)

    def test_dataset_inits(self):
        try:
            for dataset_class in self.all_dataset_classes:
                dataset_class()
        except Exception as e:
            self.fail()

    def test_valid_request(self):
        states = self.acs1.states()
        self.assertIsInstance(states, DataFrame)
        self.assertEqual(len(states), 52)
        self.assertEqual(len(states.columns), 3)

        states = self.acs1.states(return_geometry=True)
        self.assertIsInstance(states, GeoDataFrame)
        self.assertIn('geometry', states.columns)
        self.assertEqual(len(states), 52)
        self.assertEqual(len(states.columns), 4)

    def test_within_request(self):
        states = self.acs1.states(within=self.acs1.areas.region('Northeast'))
        self.assertEqual(len(states), 9)

        states = self.acs1.states(within=[self.acs1.areas.region('Northeast'), self.acs1.areas.region('South')])
        self.assertEqual(len(states), 26)

    def test_many_variables(self):
        states = self.acs1.states(variables=self.acs1.variables.filter_by_group('B01001') + self.acs1.variables.filter_by_group('B01001A'))
        self.assertEqual(len(states.columns), 83)
        self.assertIn('B01001_001E', states.columns)
        self.assertIn('B01001A_001E', states.columns)

    def test_invalid_request(self):
        with self.assertRaises(UnknownGeography) as context:
            self.acs1.blocks()


if __name__ == "__main__":
    main()