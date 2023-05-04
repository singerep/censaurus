from pyvis.network import Network
import webbrowser
import os
import queue

def get_roots(tree: dict):
    return [r for r in tree if all(r not in tree[n] for n in tree)]

def bfs(tree: dict, roots: list):
    q = queue.Queue()
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

def visualize_graph(tree: dict, labels: dict, titles: dict, hierarchical: bool = False, filename: str = ''):
    cwd = os.getcwd()

    roots = get_roots(tree)
    depths = bfs(tree, roots)
    nodes = depths.keys()

    g = Network(layout=hierarchical)

    for node in nodes:
        if depths[node] == 0:
            color = 'red'
        else:
            color = 'gray'

        if hierarchical:
            g.add_node(node, label=labels[node], title=titles[node], level=depths[node], color=color)
        else:
            g.add_node(node, label=labels[node], title=titles[node], color=color)

    for node in tree:
        for child in tree[node]:
            g.add_edge(node, child)

    g.show(filename)
    webbrowser.open('file://' + cwd + f'/{filename}')