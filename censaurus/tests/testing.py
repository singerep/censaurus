from censaurus.dataset import ACS5, Decennial

d = ACS5()
df = d.from_geography(name='tract', filters={'county': '*', 'state': '06'}, variables=d.variables.get_group('B01001'))
print(df)
# d.from_geography(name='')