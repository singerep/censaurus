from unittest import TestCase, main
from pandas import DataFrame

from censaurus.dataset import ACS1
from censaurus.utils.rename import SIMPLE_RENAMER
from censaurus.utils.regroup import AgeRegrouper


class CensusRenamerTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset = ACS1()

    def test_rename(self):
        states = self.dataset.states(variables=self.dataset.variables.filter_by_group('B01001'))
        
        states = SIMPLE_RENAMER.rename(data=states, inplace=False)
        self.assertNotIn('B01001_001E', states.columns)
        self.assertIn('sex by age|total', states.columns)

        SIMPLE_RENAMER.add_group_prefixes({'B01001': 'pop'})
        states = SIMPLE_RENAMER.rename(data=states, inplace=False)
        self.assertNotIn('sex by age|total', states.columns)
        self.assertIn('pop|total', states.columns)

        states = self.dataset.states(variables=self.dataset.variables.filter_by_group('B01001'))
        states = AgeRegrouper(['0-17', '18-29', '30-49', '50-64', '65+']).regroup(data=states, inplace=False)
        states = SIMPLE_RENAMER.rename(data=states, inplace=False)
        self.assertIn('pop|total|male|0-17', states.columns)
        self.assertNotIn('pop|total|male|0-5', states.columns)

    def test_rename_inplace(self):
        states = self.dataset.states(variables=self.dataset.variables.filter_by_group('B01001'))
        
        SIMPLE_RENAMER.rename(data=states, inplace=True)
        self.assertNotIn('sex by age|total', states.columns)
        self.assertIn('pop|total', states.columns)

        states = self.dataset.states(variables=self.dataset.variables.filter_by_group('B01001'))
        AgeRegrouper(['0-17', '18-29', '30-49', '50-64', '65+']).regroup(data=states, inplace=True)
        SIMPLE_RENAMER.rename(data=states, inplace=True)
        self.assertIn('pop|total|male|0-17', states.columns)
        self.assertNotIn('pop|total|male|0-5', states.columns)

    def test_rename_from_cdf(self):
        states = self.dataset.states(variables=self.dataset.variables.filter_by_group('B01001'))

        states.rename(census_renamer=SIMPLE_RENAMER, inplace=True)
        
        self.assertNotIn('sex by age|total', states.columns)
        self.assertIn('pop|total', states.columns)

        states = self.dataset.states(variables=self.dataset.variables.filter_by_group('B01001'))
        states.regroup(AgeRegrouper(['0-17', '18-29', '30-49', '50-64', '65+']), inplace=True)
        states.rename(SIMPLE_RENAMER, inplace=True)
        self.assertIn('pop|total|male|0-17', states.columns)
        self.assertNotIn('pop|total|male|0-5', states.columns)


if __name__ == "__main__":
    main()