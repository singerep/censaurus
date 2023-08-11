from censaurus.dataset import ACS5

dataset = ACS5()

dataset.variables.filter_by_group('B01001').visualize(filename='source/B01001.html', show=False, keep_file=True)
dataset.variables.descendants_of('B05006_002E', include_root=True).visualize(filename='source/europe.html', label_type='difference', hierarchical=True, show=False, keep_file=True, height='500px')