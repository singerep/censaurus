from censaurus.dataset import ACS5
acs = ACS5()

print(acs.variables)

print(acs.variables.filter_by_term(['sex', 'age'], by='concept'))

print(acs.groups.filter_by_term(['sex', 'age']))

acs.variables.filter_by_group(group='B01001').visualize(filename='source/B01001.html', show=False, keep_file=True, height='500px')

male_age_vars = acs.variables.children_of(variable='B01001_002E')
print(male_age_vars)

print(acs.geographies.to_df())

print(acs.geographies.get(name='tract'))

acs.geographies.visualize(filename='source/acs5_geos.html', hierarchical=True, show=False, keep_file=True, height='500px')

northeast = acs.areas.region('Northeast')

import matplotlib.pyplot as plt
northeast.plot(color='#2980b9')
plt.savefig('source/northeast.png', transparent=True, dpi=200)

data = acs.tracts(within=northeast, variables=male_age_vars, return_geometry=True)
print(data)

from censaurus.rename import SIMPLE_RENAMER

data = SIMPLE_RENAMER.rename(data)
print(data)

from censaurus.regroup import AgeRegrouper

data = AgeRegrouper(age_brackets=['0-64', '65+']).regroup(data)
data = SIMPLE_RENAMER.rename(data)

data['proportion_65+'] = data['sex by age|total|male|65+']/(data['sex by age|total|male|0-64'] + data['sex by age|total|male|65+'])

data.plot(column='proportion_65+')
plt.savefig('source/northeast_65+.png', transparent=True, dpi=200)