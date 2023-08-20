from censaurus.dataset import ACS5
from censaurus.rename import SIMPLE_RENAMER
from censaurus.regroup import AgeRegrouper

acs = ACS5()

# print(acs.variables.children_of(variable='B01001_002E'))

# print(acs.variables.filter_by_term(term=['age', 'sex'], by='concept'))

# print(acs.block_groups(within=acs.areas.place(place='Houston, TX')))

# print(acs.states(within=[acs.areas.division(division='New England'), acs.areas.division(division='Mountain')]))

counties = acs.counties(variables=acs.variables.filter_by_group('B01001'))
# print(counties)

counties = SIMPLE_RENAMER.rename(data=counties)
# print(counties)

print(list(counties.columns))

regrouper = AgeRegrouper(age_brackets=['0-17', '18-29', '30-49', '50-64', '65+'])
counties = regrouper.regroup(data=counties)
counties = SIMPLE_RENAMER.rename(data=counties)
print(list(counties.columns))