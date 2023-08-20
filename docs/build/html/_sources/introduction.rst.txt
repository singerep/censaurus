============
Introduction
============

``censaurus`` isn't just a data science tool. It's a *Census-specific data science tool*: ``censaurus`` cleverly takes advantage of the intricacies and design of the Census — all the way down to how the Census names it's variables — to let users explore, retrieve, and analyze Census data like never before. Here is an overview of some of the features ``censaurus`` provides to help you streamline your Census data workflows:

+ **Powerful variable filtering:** Like other Census tools, ``censaurus`` lets you do things like filter through available variables using a set of search terms. For example,

  .. code-block:: python  

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

  But unlike other Census tools, ``censaurus`` lets you filter variables *relationally*. For example,

  .. code-block:: python  

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
     There is also support for finding the parent, siblings, cousins, descendants, and ancestors of a particular variable. Of course, you can also filter variables by group.

  These tools come in particular handy when you want to request some subset of a large group of Census variables. Getting the entire group of variables would be unnecessary and complicate your output, but individually writing out the names of the variables you want would be tedious and error-prone.

  This behavior is available thank to the rich way ``censaurus`` processes variables. In particular, ``censaurus`` keeps track of each variable as a :class:`.Variable` object, and stores the set of variables available to a particular dataset in a :class:`.VariableCollection`.

+ **Moving beyond the Census geography hierarchy:** 

+ **Native data cleaning tools (renaming, regrouping, recoding):** 