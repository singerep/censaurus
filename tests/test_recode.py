from unittest import TestCase, main
from pandas import DataFrame

from censaurus.dataset import ACS1
from censaurus.recode import StateRecoder


class RecodeTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset = ACS1()

    def test_recode(self):
        states = self.dataset.states()
        self.assertIn('01', states['state'].values)

        states = StateRecoder().to_ABBR(data=states)
        self.assertIn('AL', states['state'].values)

        states = StateRecoder().to_FIPS(data=states)
        self.assertIn('1', states['state'].values)

        states = StateRecoder().to_GNIS(data=states)
        self.assertIn('1779775', states['state'].values)

        states = StateRecoder().to_GNIS_PADDED(data=states)
        self.assertIn('0448508', states['state'].values)

        states = StateRecoder().to_NAME(data=states)
        self.assertIn('Alabama', states['state'].values)


if __name__ == "__main__":
    main()