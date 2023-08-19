from unittest import TestCase, main

from censaurus.dataset import ACS1
from censaurus.variable import Variable, VariableCollection
from censaurus.geography import Geography


class CensusAccessorsTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.data = ACS1().states(variables=['B01001_001E'])

    def test_DataFrame_accessors(self):
        self.assertIsInstance(self.data.census.variables, VariableCollection)
        self.assertIsInstance(self.data.census.geography, Geography)

    def test_Series_accessor(self):
        self.assertIsInstance(self.data['B01001_001E'].census.variable, Variable)


if __name__ == "__main__":
    main()