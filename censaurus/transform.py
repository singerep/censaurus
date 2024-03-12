import matplotlib.pyplot as plt

from censaurus.dataset import ACS5

acs = ACS5(census_api_key='11d46bc70e375d39b67b4b4919a0099934aecbc7')
states = acs.states(variables=['B01001_001E'], return_geometry=True)
print(states)
states.plot()
plt.show()