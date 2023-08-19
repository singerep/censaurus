from unittest import TestCase, main
from pandas import DataFrame

from censaurus.dataset import ACS1
from censaurus.rename import SIMPLE_RENAMER
from censaurus.regroup import AgeRegrouper


class RenamerTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset = ACS1()

    def test_rename(self):
        states = self.dataset.states(variables=self.dataset.variables.filter_by_group('B01001'))
        
        states = SIMPLE_RENAMER.rename(data=states)
        self.assertNotIn('B01001_001E', states.columns)
        self.assertIn('sex by age|total', states.columns)

        SIMPLE_RENAMER.add_group_prefixes({'B01001': 'pop'})
        states = SIMPLE_RENAMER.rename(data=states)
        self.assertNotIn('sex by age|total', states.columns)
        self.assertIn('pop|total', states.columns)

        states = AgeRegrouper(['0-17', '18-29', '30-49', '50-64', '65+']).regroup(data=states)
        states = SIMPLE_RENAMER.rename(data=states)
        self.assertNotIn('pop|total|male|0-5', states.columns)
        self.assertIn('pop|total|male|0-17', states.columns)


if __name__ == "__main__":
    main()