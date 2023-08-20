============
Introduction
============

``censaurus`` isn't just a data science tool. It's a *Census-specific data science tool*. Leveraging the intricacies and design of the Census — down to how the Census names its variables — ``censaurus`` empowers users to explore, retrieve, and analyze Census data like never before. Key features of ``censaurus`` include:

+ **Extensive dataset support:** ``censaurus`` natively supports 17 popular Census datasets:

  * Decennial Census (general): :class:`.Decennial`
  
      + Decennial Census Redistricting Data: :class:`.DecennialPL`
      + Decennial Census Summary File 1: :class:`.DecennialSF1`
      + Decennial Census Summary File 2: :class:`.DecennialSF2`
  
  * American Community Survey Census (general): :class:`.ACS`
  
      + American Community Survey 1-Year Data: :class:`.ACS1`
      + American Community Survey 1-Year Supplemental Data: :class:`.ACSSupplemental`
      + American Community Survey 3-Year Data: :class:`.ACS3`
      + American Community Survey 5-Year Data: :class:`.ACS5`
      + American Community Survey Migration Flows: :class:`.ACSFlows`
      + American Community Survey Language Statistics: :class:`.ACSLanguage`
  
  * Public Use Microdata Sample (general): :class:`.PUMS`
  
  * Current Population Survey (general): :class:`.CPS`
  
  * Economic Census (general): :class:`.Economic`
  
      + Economic Census Key Statistics: :class:`.EconomicKeyStatistics`
  
  * Population Estimates (general): :class:`.Estimates`
  
  * Population Projections (general): :class:`.Projections`

  In addition to these built-in datasets, ``censaurus`` also supports all other Census API datasets through the generic :class:`.Dataset` class. To filter and find available datasets that meet your requirements, you can use the :class:`.DatasetExplorer` class, which comes with tools like :meth:`.DatasetExplorer.filter_by_term` and :meth:`.DatasetExplorer.filter_by_year`.

+ **Powerful variable filtering:** Utilizing the relationships between variables, ``censaurus`` grants users full control over the variable selection process. Users can easily select the exact set of variables they want, eliminating the need for the tedious and error-prone task of selecting and listing variables individually. For example,

  .. code-block:: python  

     >>> from censaurus.dataset import ACS5

     >>> acs = ACS5()
     # B01001_002E is the [sex by age -> total -> male] variable
     >>> acs.variables.children_of(variable='B01001_002E')
     VariableCollection of 23 variables:
     B01001_003E
       group: B01001
       concept: sex by age
       path: [sex by age -> estimate -> total -> male -> under 5 years]
     B01001_004E
       group: B01001
       concept: sex by age
       path: [sex by age -> estimate -> total -> male -> 5 to 9 years]
     
     ...
     
     B01001_024E
       group: B01001
       concept: sex by age
       path: [sex by age -> estimate -> total -> male -> 80 to 84 years]
     B01001_025E
       group: B01001
       concept: sex by age
       path: [sex by age -> estimate -> total -> male -> 85 years and over]

  .. note::
     There is also support for finding the parent, siblings, cousins, descendants, and ancestors of a particular variable.

  This behavior is available thanks to the rich way ``censaurus`` processes variables. In particular, ``censaurus`` keeps track of each variable as a :class:`.Variable` object, and stores the set of variables available to a particular dataset in a :class:`.VariableCollection`.

  Of course, like other Census tools, ``censaurus`` also has support for more basic variable filtering. For example,

  .. code-block:: python  

     >>> from censaurus.dataset import ACS5()
     
     >>> acs = ACS5()
     >>> acs.variables.filter_by_term(term=['age', 'sex'], by='concept')
     VariableCollection of 3417 variables:
     B01001A_001E
       group: B01001A
       concept: sex by age (white alone)
       path: [sex by age (white alone) -> estimate -> total]
     B01001A_002E
       group: B01001A
       concept: sex by age (white alone)
       path: [sex by age (white alone) -> estimate -> total -> male]
     
     ...
     
     C27009_020E
       group: C27009
       concept: va health care by sex by age
       path: [va health care by sex by age -> estimate -> total -> female -> 65 years and over -> with va health care]
     C27009_021E
       group: C27009
       concept: va health care by sex by age
       path: [va health care by sex by age -> estimate -> total -> female -> 65 years and over -> no va health care]

+ **Flexible geographic support:** ``censaurus`` lets users move beyond the Census' strict geography hierarchy. For example, within the American Community Survey, data at the ``block group``-level is traditionally only available within a specified ``state``, ``county``, or Census ``tract``. ``censaurus``, on the other hand, lets users request ``block group``-level data within *any possible geographic area*, such as ``block group``-level data within a Census ``place``:

  .. code-block:: python
     
     >>> from censaurus.dataset import ACS5

     >>> acs = ACS5()
     >>> acs.block_groups(within=acs.areas.place(place='Houston, TX'))
     successfully matched 'Houston, TX' to 'Houston city, Texas' (GEOID = 4835000) in layer 'Incorporated Places'
                                                        NAME                 GEO_ID state county   tract block group
     0     Block Group 1, Census Tract 6701.01, Fort Bend...  1500000US481576701011    48    157  670101           1
     1     Block Group 2, Census Tract 6701.01, Fort Bend...  1500000US481576701012    48    157  670101           2
     2     Block Group 3, Census Tract 6701.01, Fort Bend...  1500000US481576701013    48    157  670101           3
     3     Block Group 4, Census Tract 6701.01, Fort Bend...  1500000US481576701014    48    157  670101           4
     4     Block Group 1, Census Tract 6701.02, Fort Bend...  1500000US481576701021    48    157  670102           1
     ...                                                 ...                    ...   ...    ...     ...         ...
     2352  Block Group 2, Census Tract 6923.03, Montgomer...  1500000US483396923032    48    339  692303           2
     2353  Block Group 3, Census Tract 6923.03, Montgomer...  1500000US483396923033    48    339  692303           3
     2354  Block Group 1, Census Tract 6924.02, Montgomer...  1500000US483396924021    48    339  692402           1
     2355  Block Group 4, Census Tract 6924.02, Montgomer...  1500000US483396924024    48    339  692402           4
     2356  Block Group 5, Census Tract 6924.02, Montgomer...  1500000US483396924025    48    339  692402           5
     
     [2357 rows x 6 columns]

  .. note::
     You can even geographically subset your data with a *list* of geographic areas. This is particularly useful for comparisons. For example,

     .. code-block:: python

        >>> acs.states(within=[acs.areas.division(division='New England'), acs.areas.division(division='Mountain')])

  This feature greatly expands access to more flexible and specific geographic queries. You could even request ``block``-level data for the entire United States, if you wanted to!
  
  Internally, ``censaurus`` interfaces with the TIGERWeb API (another product of the U.S. Census Bureau) to make this behavior possible. When a user requests data inconsistent with the default geographic hierarchy, ``censaurus`` dynamically and efficiently converts that request into one (or, potentially, more than one) that *can* be properly parsed by the Census API.
  
+ **Census-focused data cleaning tools:** ``censaurus`` comes with powerful and convenient tools to help you clean and prepare your Census data for analysis. For example, you can easily rename your output columns to be more legible:

  .. code-block:: python

     >>> from censaurus.dataset import ACS5

     >>> acs = ACS5()
     >>> counties = acs.counties(variables=acs.variables.filter_by_group('B01001'))
     >>> # go from this
     >>> counties
           B01001_001E  B01001_002E  B01001_003E  B01001_004E  ...          GEO_ID                              NAME  state  county
     0           58239        28206         1783         1959  ...  0500000US01001           Autauga County, Alabama     01     001
     1          227131       110781         6121         5709  ...  0500000US01003           Baldwin County, Alabama     01     003
     2           25259        13361          647          743  ...  0500000US01005           Barbour County, Alabama     01     005
     3           22412        12300          603          646  ...  0500000US01007              Bibb County, Alabama     01     007
     4           58884        29530         1818         1906  ...  0500000US01009            Blount County, Alabama     01     009
     ...           ...          ...          ...          ...  ...             ...                               ...    ...     ...
     3216        54544        26057         1123         1277  ...  0500000US72145  Vega Baja Municipio, Puerto Rico     72     145
     3217         8317         4239          255          165  ...  0500000US72147    Vieques Municipio, Puerto Rico     72     147
     3218        22341        10796          509          664  ...  0500000US72149   Villalba Municipio, Puerto Rico     72     149
     3219        31047        15000          559          698  ...  0500000US72151    Yabucoa Municipio, Puerto Rico     72     151
     3220        34704        16548          611          699  ...  0500000US72153      Yauco Municipio, Puerto Rico     72     153
     
     [3221 rows x 53 columns]

     >>> from censaurus.renamer import SIMPLE_RENAMER

     >>> counties = SIMPLE_RENAMER.rename(data=counties)
     >>> # to this
     >>> counties
           sex by age|total  sex by age|total|male  sex by age|total|male|0-5  ...                              NAME  state  county
     0                58239                  28206                       1783  ...           Autauga County, Alabama     01     001
     1               227131                 110781                       6121  ...           Baldwin County, Alabama     01     003
     2                25259                  13361                        647  ...           Barbour County, Alabama     01     005
     3                22412                  12300                        603  ...              Bibb County, Alabama     01     007
     4                58884                  29530                       1818  ...            Blount County, Alabama     01     009
     ...                ...                    ...                        ...  ...                               ...    ...     ...
     3216             54544                  26057                       1123  ...  Vega Baja Municipio, Puerto Rico     72     145
     3217              8317                   4239                        255  ...    Vieques Municipio, Puerto Rico     72     147
     3218             22341                  10796                        509  ...   Villalba Municipio, Puerto Rico     72     149
     3219             31047                  15000                        559  ...    Yabucoa Municipio, Puerto Rico     72     151
     3220             34704                  16548                        611  ...      Yauco Municipio, Puerto Rico     72     153
     
     [3221 rows x 53 columns]

  .. note::
     The :obj:`.SIMPLE_RENAMER` can be customized to meet your needs: you can change the separator, add prefixes for specific groups, or add custom renaming functions. You can also create your own renamer from scratch using the :class:`.Renamer` class.

  Next, if the Census data you requested is too granular for your needs, you can use the regrouping tools built into ``censaurus`` to automatically aggregate your data into new, custom buckets. For example,

  .. code-block:: python

     >>> from censaurus.dataset import ACS5
     >>> from censaurus.renamer import SIMPLE_RENAMER

     >>> acs = ACS5()
     >>> counties = acs.counties(variables=acs.variables.filter_by_group('B01001'))
     >>> counties = SIMPLE_RENAMER.rename(data=counties)
     >>> # go from this
     >>> counties.columns
     Index(['sex by age|total', 'sex by age|total|male',
            'sex by age|total|male|0-5', 'sex by age|total|male|5-9',
            'sex by age|total|male|10-14', 'sex by age|total|male|15-17',
            'sex by age|total|male|18-19', 'sex by age|total|male|20',
            'sex by age|total|male|21', 'sex by age|total|male|22-24',
            'sex by age|total|male|25-29', 'sex by age|total|male|30-34',
            'sex by age|total|male|35-39', 'sex by age|total|male|40-44',
            'sex by age|total|male|45-49', 'sex by age|total|male|50-54',
            'sex by age|total|male|55-59', 'sex by age|total|male|60-61',
            'sex by age|total|male|62-64', 'sex by age|total|male|65-66',
            'sex by age|total|male|67-69', 'sex by age|total|male|70-74',
            'sex by age|total|male|75-79', 'sex by age|total|male|80-84',
            'sex by age|total|male|85+', 'sex by age|total|female',
            'sex by age|total|female|0-5', 'sex by age|total|female|5-9',
            'sex by age|total|female|10-14', 'sex by age|total|female|15-17',
            'sex by age|total|female|18-19', 'sex by age|total|female|20',
            'sex by age|total|female|21', 'sex by age|total|female|22-24',
            'sex by age|total|female|25-29', 'sex by age|total|female|30-34',
            'sex by age|total|female|35-39', 'sex by age|total|female|40-44',
            'sex by age|total|female|45-49', 'sex by age|total|female|50-54',
            'sex by age|total|female|55-59', 'sex by age|total|female|60-61',
            'sex by age|total|female|62-64', 'sex by age|total|female|65-66',
            'sex by age|total|female|67-69', 'sex by age|total|female|70-74',
            'sex by age|total|female|75-79', 'sex by age|total|female|80-84',
            'sex by age|total|female|85+', 'GEO_ID', 'NAME', 'state', 'county'],
           dtype='object')

     >>> from censaurus.regroup import AgeRegrouper
     >>> regrouper = AgeRegrouper(brackets=["0-17", "18-29", "30-49", "50-64", "65+"])
     >>> counties = regrouper.regroup(data=counties)
     >>> counties = renamer.renamer(data=counties)
     >>> # to this
     >>> counties.columns
     Index(['sex by age|total', 'sex by age|total|male',
            'sex by age|total|male|0-17', 'sex by age|total|male|18-29',
            'sex by age|total|male|30-49', 'sex by age|total|male|50-64',
            'sex by age|total|male|65+', 'sex by age|total|female',
            'sex by age|total|female|0-17', 'sex by age|total|female|18-29',
            'sex by age|total|female|30-49', 'sex by age|total|female|50-64',
            'sex by age|total|female|65+', 'GEO_ID', 'NAME', 'state', 'county'],
           dtype='object')

  .. note::
     For regrouping variables based on things other than age, you can use the generic :class:`.Regrouper` class.

  Finally, the :class:`.Recoder` class allows user to recode state names and identifiers to and from various formats.

  ``censaurus`` adds custom :obj:`pandas.DataFrame` and :obj:`pandas.Series` accessors to make this renaming, regrouping, and recoding possible.