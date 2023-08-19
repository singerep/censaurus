from unittest import TestCase, main
from pandas import DataFrame

from censaurus.dataset import ACS1
from censaurus.variable import Variable, VariableCollection


class VariableTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset = ACS1()

    def test_existing_variable(self):
        self.assertIsInstance(self.dataset.variables, VariableCollection)
        self.assertIsNotNone(self.dataset.variables.get('B01001_001E'))
        self.assertIn('sex by age', self.dataset.variables.get('B01001_001E').path)
        self.assertIsInstance(self.dataset.variables.get('B01001_001E'), Variable)

    def test_nonexisting_geography(self):
        self.assertIsNone(self.dataset.variables.get('bad_variable'))

    def test_collection_length(self):
        self.assertEqual(len(self.dataset.variables), 36428)

    def test_collection_outputs(self):
        self.assertIsInstance(self.dataset.geographies.to_df(), DataFrame)
        self.assertIsInstance(self.dataset.geographies.to_list(), list)


if __name__ == "__main__":
    main()