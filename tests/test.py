import cProfile
import pstats

with cProfile.Profile() as pr:
    import censaurus.tiger

stats = pstats.Stats(pr)
stats.sort_stats(pstats.SortKey.TIME)
stats.print_stats(10)

# acs = ACS5()
# acs.counties(variables=['B01001_001E'])

# dataset = CPS(year='2023', month='blah')
# print(dataset)