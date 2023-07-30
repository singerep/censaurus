from censaurus.dataset import ACS5
from censaurus.rename import SIMPLE_RENAMER
from censaurus.regroup import AgeRegrouper
from censaurus.recode import StateRecoder
import matplotlib.pyplot as plt

dataset = ACS5()
data = dataset.counties(variables=dataset.variables.filter_by_group('B05006'))
print(data)
# data = dataset.states(variables=['B01001_001E'])

# recoder = StateRecoder()
# data = recoder.to_NAME(data=data)

# dataset.geographies.visualize()
# data = dataset.states(variables=['EMP', 'PAYANN'], extra_census_params={'NAICS2017': '54'})
# print(data)

# dataset = Decennial(year=2010, product='cd116')
# print(dataset.variables.groups)
# dataset.variables.filter_by_group('P49').visualize(hierarchical=True, label_type='difference')
# data = dataset.counties(variables=dataset.variables.filter_by_group('B01001'))
# print(dataset.variables.to_df())