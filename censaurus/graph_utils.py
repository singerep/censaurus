from typing import Dict, List
from pyvis.network import Network
from webbrowser import open
from os import remove, getcwd
from queue import Queue
from time import sleep

def get_roots(tree: Dict[tuple, set]):
    return [r for r in tree if all(r not in tree[n] for n in tree)]

def bfs(tree: dict, roots: list):
    q = Queue()
    for r in roots:
        q.put((r, 0))
    depths = {root_node: 0 for root_node in roots}

    while not q.empty():
        node, node_depth = q.get()
        for neighbor in tree.get(node, []):
            if neighbor not in depths:
                depths[neighbor] = node_depth + 1
                q.put((neighbor, node_depth + 1))

    return depths

def visualize_graph(tree: dict, titles: dict, labels: dict, hierarchical: bool, filename: str, show: bool, keep_file: bool, **kwargs):
    if len(tree) == 0:
        print('Cannot visualize a tree with no nodes.')
        return None

    cwd = getcwd()

    roots = get_roots(tree)
    depths = bfs(tree, roots)
    nodes = depths.keys()

    width = kwargs.pop('width', '100%')
    height = kwargs.pop('height', '100%')

    g = Network(width=width, height=height, layout=hierarchical, bgcolor="#ebebeb", cdn_resources='in_line', **kwargs)

    for v_path in nodes:
        if depths[v_path] == 0:
            color = '#db7070'
        elif len(tree[v_path]) == 0:
            color = '#759bc9'
        else:
            color = 'gray'

        title = titles[v_path]
        label = labels[v_path]

        if hierarchical:
            g.add_node(title, title=title, label=label, level=depths[v_path], color=color)
        else:
            g.add_node(title, title=title, label=label, color=color)

    for v_path in tree:
        for c_path in tree[v_path]:
            v_title = titles[v_path]
            c_title = titles[c_path]
            g.add_edge(v_title, c_title)

    g.show(filename)

    if show is True:
        open('file://' + cwd + f'/{filename}')
    
    if keep_file is False:
        sleep(5)
        remove(cwd + f'/{filename}')