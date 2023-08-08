from censaurus.dataset import ACS5
# from censaurus.rename import SIMPLE_RENAMER
# from censaurus.regroup import FIVE_RACE_REGROUPER
# import matplotlib.pyplot as plt

dataset = ACS5()
dataset.variables.filter_by_term(term='race', by='concept').visualize()
# print(len(dataset.variables.descendants_of('B01001_001E')))
# print(dataset)

# dataset.variables.cousins_of('B01001_004E').visualize()
# print
# data = dataset.places(variables=['B17001_001E', 'B17001_002E', 'B25077_001E'] + dataset.variables.filter_by_group('B03002'), return_geometry=True)
# data = dataset.states(variables=['B01001_001E', 'B01001_001M'])
# print(data)

# data = FIVE_RACE_REGROUPER.regroup(data=data)

# SIMPLE_RENAMER.add_group_prefixes({'B17001': 'pov', 'B25077': 'med', 'B03002': 'pop'})
# SIMPLE_RENAMER.separator = '_'
# data = SIMPLE_RENAMER.rename(data=data)

# data['density'] = data['pop_total']/data.area
# print(data['density'])
# print(data)