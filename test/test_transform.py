from censaurus.dataset import ACS5
from censaurus.transform import comparison, continental
import matplotlib.pyplot as plt

acs = ACS5()

counties = acs.states(return_geometry=True)

# counties.clip_to_cb()

counties = continental(counties, 'state')

counties.plot(column='state')

plt.show()