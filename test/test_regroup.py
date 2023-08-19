from unittest import TestCase, main
from pandas import DataFrame

from censaurus.dataset import ACS1
from censaurus.regroup import FIVE_RACE_REGROUPER, AgeRegrouper


class RegrouperTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset = ACS1()

    def test_regroup(self):
        states = self.dataset.states(variables=self.dataset.variables.filter_by_group('B03002'))
        not_hispanic_other_sum = states[['B03002_008E', 'B03002_009E', 'B03002_005E', 'B03002_007E']].sum(axis=1)

        states_race_regrouped = FIVE_RACE_REGROUPER.regroup(data=states)
        self.assertNotIn('B03002_008E', states_race_regrouped.columns)
        self.assertTrue(all(a==b for a, b in zip(not_hispanic_other_sum, states_race_regrouped['g:B03002_005E,B03002_007E,B03002_008E,B03002_009E'])))

        states = self.dataset.states(variables=self.dataset.variables.filter_by_group('B01001'))
        male_under_18_sum = states[['B01001_003E', 'B01001_004E', 'B01001_005E', 'B01001_006E']].sum(axis=1)
        
        states_age_regrouped = AgeRegrouper(['0-17', '18-29', '30-49', '50-64', '65+']).regroup(data=states)
        self.assertNotIn('B01001_003E', states_age_regrouped.columns)
        self.assertTrue(all(a==b for a, b in zip(male_under_18_sum, states_age_regrouped['g:B01001_003E,B01001_004E,B01001_005E,B01001_006E'])))


if __name__ == "__main__":
    main()