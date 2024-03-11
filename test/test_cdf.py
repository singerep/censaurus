from unittest import TestCase, main

from censaurus.dataset import ACS5
from censaurus.cdf import CensusDataFrame, CensusGeoDataFrame, CensusSeries, GeoSeries
from censaurus.variable import VariableCollection, Variable
from censaurus.geography import Geography
from censaurus.tiger import AreaCollection


class CDFTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.cdf = ACS5().regions(variables=['B01001_001E', 'B01001_002E'])
        cls.cgdf = ACS5().regions(variables=['B01001_001E', 'B01001_002E'], area_threshold=0.01, return_geometry=True)

    def test_CensusDataFrame(self):
        self.assertIsInstance(self.cdf, CensusDataFrame)
        self.assertTrue(hasattr(self.cdf, 'variables'))
        self.assertIsInstance(self.cdf.variables, VariableCollection)
        self.assertTrue(hasattr(self.cdf, 'geography'))
        self.assertIsInstance(self.cdf.geography, Geography)

        self.assertTrue(hasattr(self.cdf[['B01001_001E', 'B01001_002E']], 'variables'))
        self.assertIsInstance(self.cdf[['B01001_001E', 'B01001_002E']].variables, VariableCollection)
        self.assertTrue(hasattr(self.cdf[['B01001_001E', 'B01001_002E']], 'geography'))
        self.assertIsInstance(self.cdf[['B01001_001E', 'B01001_002E']].geography, Geography)

    def test_CensusGeoDataFrame(self):
        self.assertIsInstance(self.cgdf, CensusGeoDataFrame)
        self.assertTrue(hasattr(self.cgdf, 'variables'))
        self.assertIsInstance(self.cgdf.variables, VariableCollection)
        self.assertTrue(hasattr(self.cgdf, 'geography'))
        self.assertIsInstance(self.cgdf.geography, Geography)
        self.assertTrue(hasattr(self.cgdf, 'areas'))
        self.assertIsInstance(self.cgdf.areas, AreaCollection)

        self.assertTrue(hasattr(self.cgdf[['B01001_001E', 'B01001_002E', 'geometry']], 'variables'))
        self.assertIsInstance(self.cgdf[['B01001_001E', 'B01001_002E', 'geometry']].variables, VariableCollection)
        self.assertTrue(hasattr(self.cgdf[['B01001_001E', 'B01001_002E', 'geometry']], 'geography'))
        self.assertIsInstance(self.cgdf[['B01001_001E', 'B01001_002E', 'geometry']].geography, Geography)
        self.assertTrue(hasattr(self.cgdf[['B01001_001E', 'B01001_002E', 'geometry']], 'areas'))
        self.assertIsInstance(self.cgdf[['B01001_001E', 'B01001_002E', 'geometry']].areas, AreaCollection)

    def test_CensusSeries(self):
        self.assertIsInstance(self.cdf['B01001_001E'], CensusSeries)
        self.assertTrue(hasattr(self.cdf['B01001_001E'], 'variable'))
        self.assertIsInstance(self.cdf['B01001_001E'].variable, Variable)

        self.assertIsInstance(self.cgdf['geometry'], GeoSeries)


if __name__ == "__main__":
    main()