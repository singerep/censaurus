from censaurus.dataset import ACS5, CPS

# dataset = CPS()
# # print(dataset.variables)
# data = dataset.states(within=dataset.areas.region('Northeast'), variables=["PEMLR","PWSSWGT","PEMARITL"], extra_census_params={'PEEDUCA':'39'})
# print(data)

dataset = ACS5()
data = dataset.counties(within=dataset.areas.region('Northeast'), variables=dataset.variables.filter_by_group('B01001') + dataset.variables.filter_by_group('B01001A'), return_geometry=True)
print(data)