import typing
from collections import defaultdict
import queue
from pyvis.network import Network
import webbrowser
import os

dir_path = os.path.dirname(os.path.realpath(__file__))


class UnknownVariable(Exception):
    pass


class Concept:
    def __init__(self, group, description) -> None:
        self.group = group
        self.description = description

    def __repr__(self) -> str:
        return f'\n{self.group}\n  description: {self.description}'


class ConceptCollection:
    def __init__(self) -> None:
        self.groups = set()
        self.concepts = []

    def add(self, concept: Concept):
        if concept.group not in self.groups:
            self.groups.add(concept.group)
            self.concepts += [concept]

    def __repr__(self):
        return str(self.concepts)

# TODO: need to be able to distinguish between E, AE, M, AM etc.
    # should also distinguish between variables that have multiple extensions, like 'Number' or 'Percent' in ACS5/2018/EEO
class Variable:
    def __init__(self, name, info) -> None:
        self.name = name
        self.info = info
        self.label = info['label']
        self.group = info['group']
        self.concept = info['concept'] if 'concept' in info else 'none'

        label_parts = self.info['label'].split('!!')
        self.type = label_parts[0]
        self.path = tuple([self.info['group']] + [p.replace(':', '') for p in label_parts[1:]])
        self.readable_path = ' -> '.join(g.replace('!', '') for g in self.path)

        self.index_path = self.parse_path(self.path)
        self.index_parent_path = self.index_path[:-1]

    def __repr__(self) -> str:
        return f'\n{self.name}\n  group: {self.group}\n  concept: {self.concept}\n  path: [{self.readable_path}]'

    @staticmethod
    def parse_path(path: tuple):
        path = tuple([p for p in path if '--' not in p])
        if path[-1] in ['Number', 'Percent']:
            path = (path[-1],) + path[:-1]
        return path

# TODO: add functionality for seeing stuff in a table? but not sure if that is necessary
# TODO: need to be able to allow pseudo-children, like the crosstab tables -- letter extension
class VariableCollection:
    def __init__(self, variables_json: dict) -> None:
        self._variables_json = variables_json
        self._variable_map = {}
        self._path_to_name_map = {}
        # TODO: need to be careful because when you access a node that doesn't exist, this indicate that it does exist but just has no children
        self._variable_tree = defaultdict(set)
        self._concepts = ConceptCollection()

        for name, info in variables_json.items():
            v = Variable(name, info)
            self._concepts.add(Concept(v.group, v.concept))
            self._variable_map[name] = v
            self._path_to_name_map[v.index_path] = name

        self.name_to_path_map = {v: k for k, v in self._path_to_name_map.items()}

        for name, variable in self._variable_map.items():
            if variable.index_parent_path in self._path_to_name_map:
                self._variable_tree[self._path_to_name_map[variable.index_parent_path]].add(name)

    def __getitem__(self, key):
        if key in self._variable_map:
            return self._variable_map[key]
        elif key in self._path_to_name_map:
            return self._variable_map[self._path_to_name_map[key]]
        else:
            raise UnknownVariable(f"variable '{key}' does not exist")

    def __iter__(self):
        return iter(self._variable_map)

    def __len__(self):
        return len(self._variable_map)

    def __add__(self, other):
        if isinstance(other, list):
            return list(self._variable_map.values()) + other

    def __radd__(self, other):
        if isinstance(other, list):
            return  other + list(self._variable_map.values())

    def __repr__(self):
        return str(list(self._variable_map.values()))

    def _build_variable_string(
        self, 
        variables: typing.Union[typing.List[str], typing.List[Variable], typing.List[typing.Union[str, Variable]], 'VariableCollection', typing.Dict[str, str]] = [],
        groups: typing.List[str] = []
    ):
        variable_names = []
        rename_map = None
        if isinstance(variables, VariableCollection):
            variable_names += variables.names
        elif all(isinstance(v, str) for v in variables):
            variable_names += variables
        elif all(isinstance(v, Variable) for v in variables):
            variable_names += [v.name for v in variables]
        elif all(isinstance(v, str) or isinstance(v, Variable) for v in variables):
            print('here')
            variable_names += [v if isinstance(v, str) else v.name for v in variables]
        elif isinstance(variables, dict) and all((isinstance(k, str) and isinstance(v, str)) for k, v in variables.items()):
            variable_names += [variables.keys()]
            rename_map = variables
        else:
            raise TypeError("the 'variables' argument only accepts one of:\n\t-a list of variable names as strings, \n\t-a list of 'Variable' objects\n\t-a mixed list of variable names as strings and 'Variable' objects\n\t-a 'VariableCollection' object\n\t-a dict of variable names as strings and strings to rename the columns")

        valid, missing = self._validate_variables(variable_names)
        if not valid:
            raise UnknownVariable(missing)

        variables = self._mask(variable_names)

        # items += [f'g({g})' for g in groups] TODO: validate groups/tables as well
        return variables, ','.join(variable_names), rename_map

    def _validate_variables(self, variable_names: typing.List[str]):
        valid = all(v in self._variable_map for v in variable_names)
        if valid:
            return True, None
        return False, [v for v in variable_names if v not in self._variable_map]
    
    def _mask(self, variables) -> 'VariableCollection':
        variables_json = {}
        for v in variables:
            variables_json[v] = self._variable_map[v].info
        
        return VariableCollection(variables_json)

    @property
    def names(self):
        return list(self._variable_map.keys())

    @property
    def variables(self):
        return list(self._variable_map.values())

    def get_group(self, group):
        return self._mask([v for v in self._variable_map if str(self._variable_map[v].group) == str(group)])

    def parent_of(self, variable) -> Variable:
        if self._variable_map[variable].index_parent_path in self._path_to_name_map:
            if self._path_to_name_map[self[variable].index_parent_path] in self._variable_map:
                return self[self._path_to_name_map[self[variable].index_parent_path]]
        return None
    
    def siblings_of(self, variable) -> 'VariableCollection':
        parent = self.parent_of(variable)
        if parent:
            return self._mask([v for v in self._variable_tree[parent.name]])
        else:
            return self._mask([])

    def children_of(self, variable, include_root: bool = True) -> 'VariableCollection':
        children = [v for v in self._variable_tree[variable]]
        if include_root:
            children += [variable]
        return self._mask(children)

    def descendants_of(self, variable, include_root: bool = True) -> 'VariableCollection':
        '''
        Returns a list of Variable objects that descend from 'variable' in BFS order
        '''
        visited_set = set()
        visited_list = []
        q = queue.Queue()

        q.put(variable)

        while not q.empty():
            v0 = q.get()
            visited_set.add(v0)
            visited_list.append(v0)
            for v1 in self._variable_tree[v0]:
                if v1 not in visited_set:
                    q.put(v1)

        return self._mask([v for v in visited_list if v != variable or include_root])

    def ancestors_of(self, variable, include_root: bool = True) -> 'VariableCollection':
        parents = [self._variable_map[variable]] if include_root else []
        while self.parent_of(variable) is not None:
            parents.append(self.parent_of(variable))
            variable = self.parent_of(variable).name
        return self._mask([v.name for v in parents])
    
    def search_variables(self, term: str, by: str = 'concept') -> 'VariableCollection':
        # TODO: should add stuff to make this exact vs non-exact, and multiple terms
        term = term.lower()
        if by == 'concept':
            return self._mask([v for v in self._variable_map if term in self._variable_map[v].concept.lower()])
        elif by == 'label':
            return self._mask([v for v in self._variable_map if term in self._variable_map[v].label.lower()])
        elif by == 'name':
            return self._mask([v for v in self._variable_map if term in self._variable_map[v].name.lower()])
        else:
            raise Exception('can only search variables by concept, label, or name')

    # TODO: should return a concept collection
    def search_concepts(self, term: str, by: str = 'description'):
        term = term.lower()
        if by == 'description':
            return [c for c in self._concepts.concepts if term in c.description.lower()]
        elif by == 'name':
            return [c for c in self._concepts.concepts if term in c.group.lower()]
        else:
            raise Exception('can only search concepts by description or name')

    def visualize(
        self, 
        variables: typing.Union['VariableCollection', typing.List['Variable']] = None,
        label_type: str = 'name',
        hierarchical: bool = False,
        filename: str = 'variable_graph.html'
    ):
        g = Network(layout=hierarchical)
        nodes = set()
        edges = set()

        if variables is None:
            variables = list(self._variable_map.values())
        elif isinstance(variables, VariableCollection):
            variables = [variables[v] for v in variables]
        elif isinstance(variables, typing.List[Variable]):
            pass
        else:
            raise Exception

        variables = set(variables)
        variable_names = set(v.name for v in variables)
        root_names = variable_names.copy()

        for v in variables:
            color = 'gray'
            if len(self._variable_tree[v.name]) > 0:
                color = 'blue'
                
            if label_type == 'name':
                label = v.name
            elif label_type == 'difference':
                label = v.index_path[-1]

            if hierarchical:
                g.add_node(v.name, title=str(v), color=color, label=label, level=len(v.path))
            else:
                g.add_node(v.name, title=str(v), color=color, label=label)

        for v in variables:
            for child in self._variable_tree[v.name]:
                if child in variable_names and (v.name, child) not in edges:
                    g.add_edge(v.name, child)
                    edges.add((v.name, child))
                if child in root_names:
                    root_names.remove(child)

        for v in root_names:
            g.get_node(v)['color'] = 'red'

        g.show(filename)
        webbrowser.open('file://' + dir_path + f'/{filename}')