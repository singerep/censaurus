from typing import List, Dict, Set, Union, Callable, Tuple
from collections import defaultdict
from queue import Queue
from os import path
from pandas import DataFrame
from itertools import zip_longest

from .graph_utils import visualize_graph

dir_path = path.dirname(path.realpath(__file__))


class UnknownGroup(Exception):
    ...


class Group:
    """
    An object that represents a single Census group (made up of Census variables).

    Parameters
    ==========
    name : :obj:`str`
        The name of the group.
    concept : :obj:`str`
        The concept (description) of the group.
    variables : :obj:`list` of :obj:`str`
        The list of variables name associated with the group.
    """
    def __init__(self, name: str, concept: str, variables: List[str]) -> None:
        self.name = name
        self.concept = concept
        self.variables = sorted(variables)

    def __repr__(self) -> str:
        if self.name and len(self.name) > 30:
            name_str =  self.name[:30] + '...'
        else:
            name_str = self.name
        if self.concept is None:
            concept_str = 'None'
        elif len(self.concept) > 100:
            concept_str =  self.name[:100] + '...'
        else:
            concept_str = self.concept
        if len(self.variables) <= 3:
            var_str = '[' + ', '.join(self.variables) + ']'
        else:
            var_str = f'[{self.variables[0]}, ..., {self.variables[-1]}]'
        return f'{name_str}\n  concept: {concept_str}\n  variables ({len(self.variables)}): {var_str}\n'


class GroupCollection:
    """
    An object that represents a collection of :class:`.Group` objects.
    """
    def __init__(self) -> None:
        self._group_map : Dict[str, Group] = {}

    def __iter__(self):
        return iter(self._group_map.values())

    def __len__(self):
        return len(self._group_map.keys())

    def __repr__(self) -> str:
        group_collection_str = f'GroupCollection of {len(self)} groups:\n'
        if len(self) > 5:
            for g in self.to_list()[:2]:
                group_collection_str += f'{g}'

            group_collection_str += '\n...\n\n'

            for g in self.to_list()[-2:]:
                group_collection_str += f'{g}'
        elif len(self) > 0:
            for g in self.to_list():
                group_collection_str += f'{g}'
        return group_collection_str

    def __contains__(self, group: str) -> bool:
        return group in self._group_map

    def get(self, group: Union[str, Group]) -> Group:
        """
        Returns the requested :class:`.Group` object if it exists. Otherwise, raises
        an :class:`.UnknownGroup` exception.

        Parameters
        ==========
        group : :obj:`str`
            The requested group.
        """
        if isinstance(group, Group):
            group = group.name
        
        if group in self._group_map:
            return self._group_map[group]
        else:
            raise UnknownGroup(f"The group '{group}' does not exist.")

    def _add(self, group: Group):
        self._group_map[group.name] = group

    def _mask(self, groups: List[str]) -> 'GroupCollection':
        groups = [g.name if isinstance(g, Group) else g for g in groups]
        gc = GroupCollection()
        for g_name in groups:
            gc._group_map[g_name] = self.get(group=g_name)
        return gc

    def filter_by_term(self, term: Union[str, List[str]]) -> 'GroupCollection':
        """
        Filters the :class:`.Group` objects based on a term or a list of terms. If
        ``term`` is a list, only groups whose concepts contain **all** terms will be
        included.

        Parameters
        ==========
        term : :obj:`str` or list :obj:`str`
            The requested search term or terms.
        """
        if isinstance(term, str):
            term = [term]
        
        terms = [t.lower() for t in term]
        g_names = [g_name for g_name in self._group_map if self._group_map[g_name].concept is not None and all(t in self._group_map[g_name].concept.lower() for t in terms)]
        return self._mask(g_names)

    def to_df(self) -> DataFrame:
        """
        Converts the :class:`.GroupCollection` into a :class:`pandas.DataFrame` object
        detailing each group's name, concept, and associated variables.
        """
        group_dicts = []
        for g_name, g in self._group_map.items():
            g_dict = {
                'name': g_name,
                'concept': g.concept,
                'variables': g.variables
            }
            group_dicts += [g_dict]

        return DataFrame(group_dicts).sort_values(by='name').reset_index(drop=True)

    def to_list(self) -> List[Group]:
        """
        Converts the :class:`.GroupCollection` into a :obj:`list` of :class:`.Group`
        objects.
        """
        return sorted(list(self._group_map.values()), key=lambda g : g.name)


class RegroupedVariable:
    """
    An object that represents a group of :class:`.Variable` objects after a regrouping
    has occurred. Not to be confused with an actual Census group. To learn more about
    custom regrouping, see :class:`.Regrouper`.

    Parameters
    ==========
    path : :obj:`tuple` of :obj:`str`
        The shared part of each path among all :class:`.Variable` objects in this 
        group.
    variables : :obj:`list` of :class:`.Variable`
        The variables associated with this group.

    Attributes
    ==========
    group : :obj:`str`
        The shared Census group among all :class:`.Variable` objects in this group.
    concept : :obj:`str`
        The shared concept among all :class:`.Variable` objects in this group.
    """
    def __init__(self, path: Tuple[str], variables: List['Variable']) -> None:
        self.path = path
        self.group = variables[0].group
        self.concept = variables[0].concept
        self.variables = variables


class VariableError(Exception):
    pass


class Variable:
    """
    An object representing a single Census variable.

    Parameters
    ==========
    name : :obj:`str`
        The name of the variable.
    info : dict of :obj:`str`: :obj:`str`
        A dictionary detailing the attributes of the variable.

    Attributes
    ==========
    label : :obj:`str` or None
        The label (description) of the variable.
    group : :obj:`str` or None
        The Census group the variable belongs to.
    concept : :obj:`str` or None
        The concept of the Census group the variable belongs to.
    type : :obj:`str` or None
        The type (int, float, etc.) of the variable.
    items : :obj:`list` of :obj:`str` or None
        Possible values of this variable. This attribute is only set for variables in
        certain datasets.
    path : :obj:`tuple` of :obj:`str` or None
        The label of the variable split up into clean, individual pieces, with the 
        variable's ``concept`` prepended. For example, the variable ``B01001_001E`` in 
        the 2021 American Community Survey 1-Year Estimates has the label 
        ``Estimate:!!Total``. This variable has a path 
        ``("sex by age", "estimate", "total")``.
    parent_path : :obj:`tuple` of :obj:`str` or None
        The path of this variable's parent variable.
    readable_path : :obj:`str`
        The elements of the path of this variable joined by " -> ". For example, the 
        variable ``B01001_001E`` in the 2021 American Community Survey 1-Year Estimates
        has a ``readable_path`` of ``sex by age -> estimate -> total``.
    """
    def __init__(self, name: str, info: Dict[str, str]) -> None:
        self.name = name
        self.info = info
        self.label = info['label'].lower() if 'label' in info else None
        self.group = info['group'] if 'group' in info else None
        if self.group == 'n/a':
            self.group = None
        self.concept = info['concept'].lower() if 'concept' in info else None
        if self.concept == 'n/a':
            self.concept = None
        self.type = int if info.get('predicateType', None) == 'int' else None
        self.items = info['values']['item'] if 'values' in info and 'item' in info['values'] else None

        if name == 'GEO_ID':
            self.label = 'GEO_ID'
            self.group = None
            self.concept = None

        self.path = None
        self.parent_path = None
        self.readable_path = None

        label_parts = self.label.split('!!')
        self._parse_label_parts(label_parts=label_parts)

        self.attributes = info.get('attributes', None)
        self._attribute_map : Dict[str, AttributeVariable] = {}
        if self.attributes is not None:
            for attr in self.attributes.split(','):
                self._attribute_map[attr] = AttributeVariable(name=attr, owner=self)

    def __repr__(self) -> str:
        var_str = f'{self.name}\n  group: {self.group}\n  concept: {self.concept}\n  path: [{self.readable_path}]\n'
        if self.items is not None:
            items_str = ', '.join([f'{v} ({k})' for k, v in list(self.items.items())[:5]])
            if len(self.items) > 5:
                items_str += ', ...'
            var_str += f'  items ({len(self.items)}): {items_str}\n'
                
        return var_str

    def _parse_label_parts(self, label_parts: List[str]) -> None:
        self.path = tuple([p.replace(':', '') for p in label_parts])
        if self.concept is not None:
            self.path = (self.concept,) + self.path
        self.parent_path = self.path[:-1]
        self.readable_path = ' -> '.join(g.replace('!', '') for g in self.path)


class AttributeVariable(Variable):
    def __init__(self, name: str, owner: Variable) -> None:
        for i, (n_c, o_c) in enumerate(zip_longest(name, owner.name)):
            if n_c != o_c:
                break
        self.attribute_type = name[i:]
        if self.attribute_type == 'A':
            self.attribute_type = 'annotation'
        elif self.attribute_type == 'M':
            self.attribute_type = 'margin of error'
        elif self.attribute_type == 'MA':
            self.attribute_type = 'annotation of margin of error'

        self.name = name
        self.label = owner.label
        self.group = owner.group
        self.concept = owner.concept
        self.type = owner.type
        self.items = owner.items
        self.path = owner.path + (self.attribute_type,)
        self.parent_path = owner.parent_path
        self.readable_path = owner.readable_path + f' -> {self.attribute_type}'

        if self.name == 'NAME':
            self.label = 'NAME'
            self.path = ('NAME',)
            self.readable_path = 'NAME'
        

class VariableCollection:
    """
    An object that represents a collection of :class:`.Variable` objects.

    Parameters
    ==========
    variables_json : dict of :obj:`str`: (dict of :obj:`str`: :obj:`str`)
        A dictionary detailing the attributes of each variable.
    """
    def __init__(self, variables_json: Dict[str, Dict[str, str]]) -> None:
        self._variable_map : Dict[str, Variable] = {}
        self._path_to_name_map : Dict[tuple, str] = {}
        self._variable_tree : Dict[tuple, Set[tuple]] = {}
        self._attribute_map : Dict[str, str] = {}

        self._group_map = defaultdict(set)
        self._group_collection = GroupCollection()

        variable_names = variables_json.keys()
        for v_name in sorted(variable_names):
            v_info = variables_json[v_name]
            v = Variable(name=v_name, info=v_info)
            self._variable_map[v_name] = v
            self._path_to_name_map[v.path] = v_name
            self._variable_tree[v.path] = set()
            
            if v.parent_path in self._variable_tree:
                self._variable_tree[v.parent_path].add(v.path)

            self._group_map[(v.group, v.concept)].add(v_name)
            
            if v.attributes is not None:
                for attr in v.attributes.split(','):
                    self._attribute_map[attr] = v.name

        for (group, concept), variables in self._group_map.items():
            g = Group(name=group, concept=concept, variables=variables)
            self._group_collection._add(group=g)

    def __iter__(self):
        return iter(self._variable_map.values())

    def __len__(self):
        return len(self._variable_map)

    def __add__(self, other):
        if isinstance(other, list):
            return list(self._variable_map.keys()) + other
        elif isinstance(other, VariableCollection):
            return list(self._variable_map.keys()) + list(other._variable_map.keys())
        else:
            raise Exception('Can only add objects of type list or VariableCollection to another VariableCollection')

    def __radd__(self, other):
        if isinstance(other, list):
            return  other + list(self._variable_map.keys())

        if isinstance(other, list):
            return other + list(self._variable_map.keys())
        elif isinstance(other, VariableCollection):
            return list(other._variable_map.keys()) + list(self._variable_map.keys())
        else:
            raise Exception('Can only add objects of type list or VariableCollection to another VariableCollection')

    def __repr__(self):
        var_collection_str = f'VariableCollection of {len(self)} variables:\n'
        if len(self) > 5:
            for v in self.to_list()[:2]:
                var_collection_str += f'{v}'

            var_collection_str += '\n...\n\n'

            for v in self.to_list()[-2:]:
                var_collection_str += f'{v}'
        elif len(self) > 0:
            for v in self.to_list():
                var_collection_str += f'{v}'
        return var_collection_str

    def _build_variable_params(self, variables: Union['VariableCollection', List[str], List[Variable], List[Union[str, Variable]], Dict[str, str]] = [], groups: List[str] = []):
        variable_names = []

        rename_map = {}
        if isinstance(variables, VariableCollection):
            variable_names += variables.names
        elif isinstance(variables, list) and all(isinstance(v, str) for v in variables):
            variable_names += variables
        elif isinstance(variables, list) and all(isinstance(v, Variable) for v in variables):
            variable_names += [v.name for v in variables]
        elif isinstance(variables, list) and all(isinstance(v, str) or isinstance(v, Variable) for v in variables):
            variable_names += [v if isinstance(v, str) else v.name for v in variables]
        elif isinstance(variables, dict) and all((isinstance(k, str) and isinstance(v, str)) for k, v in variables.items()):
            variable_names += list(variables.keys())
            rename_map = variables
        else:
            raise TypeError("the 'variables' argument only accepts one of:\n\t-a list of variable names as strings, \n\t-a list of 'Variable' objects\n\t-a mixed list of variable names as strings and 'Variable' objects\n\t-a 'VariableCollection' object\n\t-a dict of variable names as strings and strings to rename the columns")

        valid, missing = self._validate_variables(variable_names=variable_names)
        if not valid:
            raise VariableError(f'The following variables do not exist: {missing}')

        valid, missing = self._validate_groups(groups=groups)
        if not valid:
            raise UnknownGroup(f'The following groups do not exist: {missing}')

        for g in groups:
            variable_names += self.filter_by_group(group=g)

        variable_names_set = set()
        unique_variable_names = []
        for v_n in variable_names:
            if v_n not in variable_names_set:
                variable_names_set.add(v_n)
                unique_variable_names.append(v_n)
            
        variables = self._mask(unique_variable_names)

        if 'NAME' not in unique_variable_names and self.get('NAME'):
            unique_variable_names.append('NAME')
        if 'GEO_ID' not in unique_variable_names and self.get('GEO_ID'):
            unique_variable_names.append('GEO_ID')

        chunk_size = 48
        chunks = [unique_variable_names[i:i + chunk_size] for i in range(0, len(unique_variable_names), chunk_size)]
        variable_params_list = []
        for chunk in chunks:
            if 'GEO_ID' not in chunk and 'GEO_ID' in unique_variable_names:
                chunk.append('GEO_ID')
            if 'NAME' not in chunk and 'NAME' in unique_variable_names:
                chunk.append('NAME')
            variable_params_list.append({'get': ','.join(chunk)})

        return variables, variable_params_list, rename_map

    def _validate_variables(self, variable_names: List[str]):
        valid = all(self.get(variable=v) is not None for v in variable_names)
        if valid:
            return True, None
        return False, [v for v in variable_names if v not in self._variable_map]

    def _validate_groups(self, groups: List[str]):
        valid = all(g in self._group_collection for g in groups)
        if valid:
            return True, None
        return False, [g for g in groups if g not in self._group_map]
    
    def _mask(self, variables: List[Union[str, Variable]]) -> 'VariableCollection':
        variables = [v.name if isinstance(v, Variable) else v for v in variables]

        variables_json = {}
        for v_name in variables:
            v = self.get(variable=v_name)
            if v is not None:
                variables_json[v_name] = v.info
        
        return VariableCollection(variables_json)

    @property
    def names(self) -> List[str]:
        """
        A list of the names of each variable in the collection.
        """
        return list(self._variable_map.keys())

    @property
    def groups(self) -> GroupCollection:
        """
        The collection of groups associated with the variables in this collection.
        """
        return self._group_collection

    def get(self, variable: Union[str, Variable]) -> Variable:
        """
        Returns the requested :class:`.Variable` object if it exists. Otherwise, returns
        ``None``.

        Parameters
        ==========
        variable : :obj:`str` or :class:`.Variable`
            The requested variable.
        """
        if isinstance(variable, Variable):
            variable = variable.name

        if variable in self._variable_map:
            return self._variable_map.get(variable)
        elif variable in self._attribute_map:
            return self._variable_map.get(self._attribute_map.get(variable))._attribute_map.get(variable)
        return None

    def parent_of(self, variable: Union[str, Variable]) -> Variable:
        """
        Returns the parent of the requested variable.

        For example, the variable ``B01001_002`` 
        (path = ``(sex by age, estimate, total, male)``) has the parent ``B01001_001``
        (path = ``(sex by age, estimate, total)``).

        Parameters
        ==========
        variable : :obj:`str` or :class:`.Variable`
            The requested variable.
        """
        v = self.get(variable=variable)
        if v.parent_path in self._path_to_name_map:
            p_name = self._path_to_name_map[v.parent_path]
            return self.get(p_name)
        
        return None
    
    def siblings_of(self, variable: Union[str, Variable], include_root: bool = False) -> 'VariableCollection':
        """
        Returns the siblings of the requested variable as a new 
        :class:`.VariableCollection`.

        For example, the variable ``B01001_003`` 
        (path = ``(sex by age, estimate, total, male, under 5 years)``) has 22 siblings,
        including:

           + ``B01001_004`` (path = ``(sex by age, estimate, total, male, 6 to 9 years)``).
           + ``B01001_005`` (path = ``(sex by age, estimate, total, male, 10 to 14 years)``).
           + ...
           + ``B01001_024`` (path = ``(sex by age, estimate, total, male, 80 to 84 years)``).
           + ``B01001_025`` (path = ``(sex by age, estimate, total, male, 85 years and over)``).

        Parameters
        ==========
        variable : :obj:`str` or :class:`.Variable`
            The requested variable.
        include_root : :obj:`bool` = False
            Determines whether or not the requested variable will be included in the
            returned :class:`.VariableCollection`.
        """
        v = self.get(variable=variable)
        parent = self.parent_of(variable)
        if parent:
            v_paths = [v_path for v_path in self._variable_tree[parent.path] if (v_path != v.path) or include_root]
            v_names = [self._path_to_name_map[v_path] for v_path in v_paths]
            return self._mask(v_names)
        else:
            return self._mask([])

    def siblings_and_cousins_of(self, variable: Union[str, Variable], include_root: bool = False) -> 'VariableCollection':
        """
        Returns the siblings and cousins of the requested variable as a new 
        :class:`.VariableCollection`.

        For example, the variable ``B01001_003`` 
        (path = ``(sex by age, estimate, total, male, under 5 years)``) has 45 siblings
        and cousins, including:

           + ``B01001_004`` (path = ``(sex by age, estimate, total, male, 6 to 9 years)``).
           + ``B01001_005`` (path = ``(sex by age, estimate, total, male, 10 to 14 years)``).
           + ...
           + ``B01001_024`` (path = ``(sex by age, estimate, total, male, 80 to 84 years)``).
           + ``B01001_025`` (path = ``(sex by age, estimate, total, male, 85 years and over)``).
           + ``B01001_027`` (path = ``(sex by age, estimate, total, female, under 5 years)``).
           + ``B01001_028`` (path = ``(sex by age, estimate, total, female, 6 to 9 years)``).
           + ...
           + ``B01001_048`` (path = ``(sex by age, estimate, total, female, 80 to 84 years)``).
           + ``B01001_049`` (path = ``(sex by age, estimate, total, female, 85 years and over)``).

        Parameters
        ==========
        variable : :obj:`str` or :class:`.Variable`
            The requested variable.
        include_root : :obj:`bool` = False
            Determines whether or not the requested variable will be included in the
            returned :class:`.VariableCollection`.
        """
        root = self.root_of(variable=variable)
        root_descendants = self.descendants_of(variable=root, include_root=True)
        v = self.get(variable=variable)
        v_depth = len(v.path)
        cousin_names = [c.name for c in root_descendants if len(c.path) == v_depth and (c.path != v.path or include_root)]
        return self._mask(cousin_names)


    def children_of(self, variable: Union[str, Variable], include_root: bool = False) -> 'VariableCollection':
        """
        Returns the children of the requested variable as a new 
        :class:`.VariableCollection`.

        For example, the variable ``B01001_002`` 
        (path = ``(sex by age, estimate, total, male)``) has 23 children, including:

           + ``B01001_004`` (path = ``(sex by age, estimate, total, male, under 5 years)``).
           + ``B01001_004`` (path = ``(sex by age, estimate, total, male, 6 to 9 years)``).
           + ...
           + ``B01001_024`` (path = ``(sex by age, estimate, total, male, 80 to 84 years)``).
           + ``B01001_025`` (path = ``(sex by age, estimate, total, male, 85 years and over)``).

        Parameters
        ==========
        variable : :obj:`str` or :class:`.Variable`
            The requested variable.
        include_root : :obj:`bool` = False
            Determines whether or not the requested variable will be included in the
            returned :class:`.VariableCollection`.
        """
        v = self.get(variable=variable)
        v_paths = [v_path for v_path in self._variable_tree[v.path]]
        v_names = [self._path_to_name_map[v_path] for v_path in v_paths]
        if include_root:
            v_names = [variable] + v_names
        return self._mask(v_names)

    def descendants_of(self, variable: Union[str, Variable], include_root: bool = False) -> 'VariableCollection':
        """
        Returns the descendants of the requested variable as a new 
        :class:`.VariableCollection`.

        For example, the variable ``B01001_001`` 
        (path = ``(sex by age, estimate, total)``) has 48 descendants, including:

           + ``B01001_002`` (path = ``(sex by age, estimate, total, male)``).
           + ``B01001_003`` (path = ``(sex by age, estimate, total, male, under 5 years)``).
           + ``B01001_004`` (path = ``(sex by age, estimate, total, male, 6 to 9 years)``).
           + ...
           + ``B01001_024`` (path = ``(sex by age, estimate, total, male, 80 to 84 years)``).
           + ``B01001_025`` (path = ``(sex by age, estimate, total, male, 85 years and over)``).
           + ``B01001_026`` (path = ``(sex by age, estimate, total, female)``).
           + ``B01001_027`` (path = ``(sex by age, estimate, total, female, under 5 years)``).
           + ``B01001_028`` (path = ``(sex by age, estimate, total, female, 6 to 9 years)``).
           + ...
           + ``B01001_048`` (path = ``(sex by age, estimate, total, female, 80 to 84 years)``).
           + ``B01001_049`` (path = ``(sex by age, estimate, total, female, 85 years and over)``).

        Parameters
        ==========
        variable : :obj:`str` or :class:`.Variable`
            The requested variable.
        include_root : :obj:`bool` = False
            Determines whether or not the requested variable will be included in the
            returned :class:`.VariableCollection`.
        """
        visited_set = set()
        q = Queue()

        q.put(variable)

        while not q.empty():
            v_name = q.get()
            visited_set.add(v_name)

            v = self.get(v_name)
            for c_path in self._variable_tree[v.path]:
                c_name = self._path_to_name_map[c_path]
                if c_name not in visited_set:
                    q.put(c_name)

        return self._mask([v for v in visited_set if (v != variable) or include_root])

    def first_ancestor_of(self, variable: Union[str, Variable]) -> Variable:
        """
        Returns the first (root) ancestor of the requested variable.

        For example, the variable ``B01001_003`` 
        (path = ``(sex by age, estimate, total, male, under 5 years)``) has a first
        ancestor of ``B01001_001`` (path = ``(sex by age, estimate, total)``).

        Parameters
        ==========
        variable : :obj:`str` or :class:`.Variable`
            The requested variable.
        """
        while self.parent_of(variable=variable) is not None:
            variable = self.parent_of(variable=variable)
        return variable

    def ancestors_of(self, variable: Union[str, Variable], include_root: bool = False) -> 'VariableCollection':
        """
        Returns the ancestors of the requested variable as a new 
        :class:`.VariableCollection`.

        For example, the variable ``B01001_003`` 
        (path = ``(sex by age, estimate, total, under 5 years)``) has 2 ancestors:

           + ``B01001_001`` (path = ``(sex by age, estimate, total)``).
           + ``B01001_002`` (path = ``(sex by age, estimate, total, male)``).

        Parameters
        ==========
        variable : :obj:`str` or :class:`.Variable`
            The requested variable.
        include_root : :obj:`bool` = False
            Determines whether or not the requested variable will be included in the
            returned :class:`.VariableCollection`.
        """
        if include_root:
            parents = [variable]
        else:
            parents = []
        while self.parent_of(variable=variable) is not None:
            p = self.parent_of(variable)
            parents.append(p.name)
            variable = p.name

        return self._mask(parents)

    def filter_by_term(self, term: Union[str, List[str]], by: str = 'label') -> 'VariableCollection':
        """
        Returns a new :class:`.VariableCollection` consisting of all variables
        that match the search. Can filter by each variable's label or by the
        concept of each variable's group.

        Parameters
        ==========
        term : :obj:`str` or :obj:`list` of :obj:`str`
            The search string or strings.
        by : :obj:`str` = 'label'
            If ``by`` is 'label', then variables will be filtered by their labels.
            Otherwise, ``by`` should be 'concept', and variables will be filtered
            by the concepts of their groups.
        """
        if isinstance(term, str):
            term = [term]
        
        terms = [t.lower() for t in term]
        if by == 'label':
            v_names = [v_name for v_name in self._variable_map if all(t in self._variable_map[v_name].label.lower() for t in terms)]
            return self._mask(v_names)
        elif by == 'concept':
            v_names = [v_name for v_name in self._variable_map if all(self._variable_map[v_name].concept is not None and t in self._variable_map[v_name].concept.lower() for t in terms)]
            return self._mask(v_names)
        else:
            raise ValueError("'by' should either be 'label' or 'concept'")

    def filter_by_group(self, group: Union[str, Group]) -> 'VariableCollection':
        """
        Returns a new :class:`.VariableCollection` consisting of all variables within 
        the given group.

        Parameters
        ==========
        group : :obj:`str` or :class:`.Group`
            The group to return.
        """
        g = self.groups.get(group=group)
        v_names = [v_name for v_name in self._variable_map if self._variable_map[v_name].group == g.name]
        return self._mask(v_names)

    def to_df(self) -> DataFrame:
        """
        Converts the :class:`.VariableCollection` into a :class:`pandas.DataFrame` 
        object detailing each variable's name, label, group, concept, type, items,
        attributes, and path.
        """
        var_dicts = []
        for v_name, v in self._variable_map.items():
            v_dict = {
                'name': v_name,
                'label': v.readable_path,
                'group': v.group,
                'concept': v.concept,
                'type': v.type,
                'items': v.items,
                'attributes': v.attributes,
                'path': v.path
            }
            var_dicts += [v_dict]

        return DataFrame(var_dicts)

    def to_list(self) -> List[Variable]:
        """
        Converts the :class:`.VariableCollection` into a :obj:`list` of :class:`.Variable` 
        objects.
        """
        return list(self._variable_map.values())

    def visualize(self, label_type: str = 'name', hierarchical: bool = False, filename: str = 'variable_graph.html', show: bool = True, keep_file: bool = False, **kwargs):
        """
        Visualizes the :class:`.VariableCollection` as a tree in your default
        webbrowser.

        Parameters
        ==========
        label_type : :obj:`str` = 'name'
            Controls the labels of each variable (node). If ``label_type`` equals
            'name', then the label of each variable is its name (``B01001_001E``,
            ``B01001_002E``, ``B01001_003E``, etc.). If ``label_type`` equals
            'difference', then the label of each variable is the last element of its
            path (``total``, ``male``, ``under 5 years``, etc.). 'difference' shows the
            difference between each variable and its parent.
        hierarchical : :obj:`bool` = False
            Determines whether the variables (nodes) are presented in a hierarchical
            layout (with root nodes at the top), as opposed to a layout that looks
            more like spokes on a wheel.
        filename : :obj:`str` = 'variable_graph.html'
            The path (from within the current working directory) the save the generated
            file at.
        show : :obj:`bool` = True
            Determines whether or not to open the generated file in your default
            webbrowser.
        keep_file : :obj:`bool` = False
            Determines whether or not to delete the generated file after opening it.
        """
        if label_type == 'name':
            labels = self._path_to_name_map
        elif label_type == 'difference':
            labels = {v.path: v.path[-1] for k, v in self._variable_map.items()}
        else:
            raise ValueError("'label_type' should be either 'name' or 'difference'")

        titles = {v_path: str(self.get(self._path_to_name_map[v_path])) for v_path in self._variable_tree}

        visualize_graph(tree=self._variable_tree, titles=titles, labels=labels, hierarchical=hierarchical, filename=filename, show=show, keep_file=keep_file, **kwargs)