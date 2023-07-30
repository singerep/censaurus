from typing import List, Dict, Set, Union, Callable, Tuple
from collections import defaultdict
import queue
import os
import re
import pandas as pd

from .graph_utils import visualize_graph

dir_path = os.path.dirname(os.path.realpath(__file__))


class UnknownGroup(Exception):
    ...


class Group:
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
    def __init__(self) -> None:
        self._group_map : Dict[str, Group] = {}

    def __iter__(self):
        return iter(self._group_map.values())

    def __len__(self):
        return len(self._group_map.keys())

    def __repr__(self) -> str:
        gs = self._group_map.values()
        return_str = ''
        for g in gs:
            return_str += str(g)
        return return_str

    def get(self, group: str) -> Group:
        if group in self._group_map:
            return self._group_map[group]

        else:
            raise UnknownGroup(f"The group '{group}' does not exist.")

    def add(self, group: Group):
        self._group_map[group.name] = group

    def _mask(self, groups: List[str]) -> 'GroupCollection':
        gc = GroupCollection()
        for g_name in groups:
            gc._group_map[g_name] = self.get(group=g_name)
        return gc

    def filter_by_term(self, term: str):
        term = term.lower()
        g_names = [g_name for g_name in self._group_map if self._group_map[g_name].concept is not None and term in self._group_map[g_name].concept.lower()]
        return self._mask(g_names)

    def to_df(self) -> pd.DataFrame:
        group_dicts = []
        for g_name, g in self._group_map.items():
            g_dict = {
                'name': g_name,
                'concept': g.concept,
                'variables': g.variables
            }
            group_dicts += [g_dict]

        return pd.DataFrame(group_dicts)

    def to_list(self) -> List[Group]:
        return list(self._group_map.values())


class RegroupedVariable:
    def __init__(self, path: Tuple[str], variables: List['Variable']) -> None:
        self.path = path
        self.group = variables[0].group
        self.concept = variables[0].concept
        self.variables = variables


class VariableError(Exception):
    pass


class Variable:
    def __init__(self, name: str, info: dict) -> None:
        self.name = name
        self.info = info
        self.label = info['label'].lower() if 'label' in info else None
        self.group = info['group'] if 'group' in info else None
        if self.group == 'n/a':
            self.group = None
        self.concept = info['concept'].lower() if 'concept' in info else None
        if self.concept == 'n/a':
            self.concept = None

        if name == 'GEO_ID':
            self.label = 'GEO_ID'
            self.group = None
            self.concept = None
        
        # TODO: need to search these
        self.attributes = info.get('attributes', None)

        self.path = None
        self.parent_path = None
        self.readable_path = None

        label_parts = self.label.split('!!')
        self.parse_label_parts(label_parts=label_parts)

    def __repr__(self) -> str:
        return f'{self.name}\n  group: {self.group}\n  concept: {self.concept}\n  path: [{self.readable_path}]\n'

    def parse_label_parts(self, label_parts: List[str]) -> None:
        # TODO: this could get handed down from the initial call so that datasets could have custom parsing
        self.path = tuple([p.replace(':', '') for p in label_parts])
        if self.concept is not None:
            self.path = (self.concept,) + self.path
        self.parent_path = self.path[:-1]
        self.readable_path = ' -> '.join(g.replace('!', '') for g in self.path)


class VariableCollection:
    def __init__(self, variables_json: Dict[str, dict]) -> None:
        self._variable_map : Dict[str, Variable] = {}
        self._path_to_name_map : Dict[tuple, str] = {}
        self._variable_tree : Dict[tuple, Set[tuple]] = {}

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
# 
        for (group, concept), variables in self._group_map.items():
            g = Group(name=group, concept=concept, variables=variables)
            self._group_collection.add(group=g)

    def __iter__(self):
        return iter(self._variable_map.values())

    def __len__(self):
        return len(self._variable_map)

    def __add__(self, other):
        if isinstance(other, list):
            return list(self._variable_map.values()) + other
        elif isinstance(other, VariableCollection):
            return list(self._variable_map.keys()) + list(other._variable_map.keys())
        else:
            raise Exception('Can only add objects of type list or VariableCollection to another VariableCollection')

    def __radd__(self, other):
        if isinstance(other, list):
            return  other + list(self._variable_map.values())

        if isinstance(other, list):
            return other + list(self._variable_map.values())
        elif isinstance(other, VariableCollection):
            return list(other._variable_map.keys()) + list(self._variable_map.keys())
        else:
            raise Exception('Can only add objects of type list or VariableCollection to another VariableCollection')

    def __repr__(self):
        vs = self._variable_map.values()
        return_str = ''
        for v in vs:
            return_str += str(v)
        return return_str

    def _build_variable_params(self, variables: Union['VariableCollection', List[str], List[Variable], List[Union[str, Variable]], Dict[str, str]] = [], groups: List[str] = []):
        variable_names = []

        rename_map = {}
        if isinstance(variables, VariableCollection):
            variable_names += variables.names
        elif isinstance(variables, list) and all(isinstance(v, str) for v in variables):
            variable_names += variables
        elif isinstance(variables, Variable) and all(isinstance(v, Variable) for v in variables):
            variable_names += [v.name for v in variables]
        elif isinstance(variables, list) and all(isinstance(v, str) or isinstance(v, Variable) for v in variables):
            variable_names += [v if isinstance(v, str) else v.name for v in variables]
        elif isinstance(variables, dict) and all((isinstance(k, str) and isinstance(v, str)) for k, v in variables.items()):
            variable_names += list(variables.keys())
            rename_map = variables
        else:
            raise TypeError("the 'variables' argument only accepts one of:\n\t-a list of variable names as strings, \n\t-a list of 'Variable' objects\n\t-a mixed list of variable names as strings and 'Variable' objects\n\t-a 'VariableCollection' object\n\t-a dict of variable names as strings and strings to rename the columns")

        valid, missing = self._validate_variables(variable_names)
        if not valid:
            raise VariableError(f'The following variables do not exist: {missing}')

        variable_names_set = set()
        unique_variable_names = []
        for v_n in variable_names:
            if v_n not in variable_names_set:
                variable_names_set.add(v_n)
                unique_variable_names.append(v_n)
            
        variables = self._mask(unique_variable_names)

        if 'NAME' not in unique_variable_names:
            unique_variable_names.append('NAME')
        if 'GEO_ID' not in unique_variable_names:
            unique_variable_names.append('GEO_ID')

        chunk_size = 49
        chunks = [unique_variable_names[i:i + chunk_size] for i in range(0, len(unique_variable_names), chunk_size)]
        variable_params_list = []
        for chunk in chunks:
            if 'GEO_ID' not in chunk:
                chunk.append('GEO_ID')
            variable_params_list.append({'get': ','.join(chunk)})

        return variables, variable_params_list, rename_map

    def _validate_variables(self, variable_names: List[str]):
        valid = all(v in self._variable_map for v in variable_names)
        if valid:
            return True, None
        return False, [v for v in variable_names if v not in self._variable_map]
    
    def _mask(self, variables: List[str]) -> 'VariableCollection':
        variables_json = {}
        for v in variables:
            variables_json[v] = self._variable_map[v].info
        
        return VariableCollection(variables_json)

    @property
    def names(self) -> List[str]:
        return list(self._variable_map.keys())

    @property
    def groups(self) -> GroupCollection:
        return self._group_collection

    def get(self, variable: str) -> Variable:
        return self._variable_map.get(variable, None)

    def parent_of(self, variable: str) -> Variable:
        v = self.get(variable=variable)
        if v.parent_path in self._path_to_name_map:
            p_name = self._path_to_name_map[v.parent_path]
            return self.get(p_name)
        
        return None
    
    def siblings_of(self, variable: str, include_root: bool = False) -> 'VariableCollection':
        v = self.get(variable=variable)
        parent = self.parent_of(variable)
        if parent:
            v_paths = [v_path for v_path in self._variable_tree[parent.path] if (v_path != v.path) or include_root]
            v_names = [self._path_to_name_map[v_path] for v_path in v_paths]
            return self._mask(v_names)
        else:
            return self._mask([])

    def children_of(self, variable: str, include_root: bool = False) -> 'VariableCollection':
        v = self.get(variable=variable)
        v_paths = [v_path for v_path in self._variable_tree[v.path]]
        v_names = [self._path_to_name_map[v_path] for v_path in v_paths]
        if include_root:
            v_names = [variable] + v_names
        return self._mask(v_names)

    def descendants_of(self, variable: str, include_root: bool = False) -> 'VariableCollection':
        '''
        Returns a list of Variable objects that descend from 'variable' in BFS order
        '''
        visited_set = set()
        q = queue.Queue()

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

    def ancestors_of(self, variable: str, include_root: bool = False) -> 'VariableCollection':
        if include_root:
            parents = [variable]
        else:
            parents = []
        while self.parent_of(variable=variable) is not None:
            p = self.parent_of(variable)
            parents.append(p.name)
            variable = p.name

        return self._mask(parents)

    def filter_by_term(self, term: str, by: str = 'label') -> 'VariableCollection':
        term = term.lower()
        if by == 'label':
            v_names = [v_name for v_name in self._variable_map if term in self._variable_map[v_name].label.lower()]
            return self._mask(v_names)
        elif by == 'concept':
            v_names = [v_name for v_name in self._variable_map if self._variable_map[v_name].concept is not None and term in self._variable_map[v_name].concept.lower()]
            return self._mask(v_names)

    def filter_by_group(self, group: str) -> 'VariableCollection':
        g = self.groups.get(group=group)
        v_names = [v_name for v_name in self._variable_map if self._variable_map[v_name].group == g.name]
        return self._mask(v_names)

    def visualize(self, label_type: str = 'name', hierarchical: bool = False, filename: str = 'variable_graph.html'):
        if label_type == 'name':
            labels = self._path_to_name_map
        elif label_type == 'difference':
            labels = {v.path: v.path[-1] for k, v in self._variable_map.items()}

        titles = {v_path: str(self.get(self._path_to_name_map[v_path])) for v_path in self._variable_tree}

        visualize_graph(tree=self._variable_tree, titles=titles, labels=labels, hierarchical=hierarchical, filename=filename)

    def to_df(self) -> pd.DataFrame:
        var_dicts = []
        for v_name, v in self._variable_map.items():
            v_dict = {
                'name': v_name,
                'label': v.readable_path,
                'group': v.group,
                'concept': v.concept,
                'attributes': v.attributes
            }
            var_dicts += [v_dict]

        return pd.DataFrame(var_dicts)

    def to_list(self) -> List[Variable]:
        return list(self._variable_map.values())