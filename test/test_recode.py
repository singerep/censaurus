from unittest import TestCase, main
from pandas import DataFrame

from censaurus.dataset import ACS1
from censaurus.utils.recode import StateRecoder


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

    def test_recode_inplace(self):
        states = self.dataset.states()
        self.assertIn('01', states['state'].values)

        StateRecoder().to_ABBR(data=states, inplace=True)
        self.assertIn('AL', states['state'].values)

        StateRecoder().to_FIPS(data=states, inplace=True)
        self.assertIn('1', states['state'].values)

        StateRecoder().to_GNIS(data=states, inplace=True)
        self.assertIn('1779775', states['state'].values)

        StateRecoder().to_GNIS_PADDED(data=states, inplace=True)
        self.assertIn('0448508', states['state'].values)

        StateRecoder().to_NAME(data=states, inplace=True)
        self.assertIn('Alabama', states['state'].values)

    def test_recode_from_cdf(self):
        states = self.dataset.states()
        self.assertIn('01', states['state'].values)

        states.recode('ABBR', inplace=True)
        self.assertIn('AL', states['state'].values)

        states.recode('FIPS', inplace=True)
        self.assertIn('1', states['state'].values)

        states.recode('GNIS', inplace=True)
        self.assertIn('1779775', states['state'].values)

        states.recode('GNIS_PADDED', inplace=True)
        self.assertIn('0448508', states['state'].values)

        states.recode('NAME', inplace=True)
        self.assertIn('Alabama', states['state'].values)


if __name__ == "__main__":
    main()