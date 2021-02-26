import pdb
class Node():
    def __init__(self, name):
        self.name = name
        self.alpha = float('-inf')
        self.beta = float('inf')
        self.utility = None

    def ap(self, v):
        if v==float('inf'): return 'inf'
        elif v==float('-inf'): return '-inf' 
        else: return str(v)

    def __repr__(self): return f"{self.name} - [ {self.ap(self.alpha)}, {self.ap(self.beta)} ]"


tree = {
    'A' : ['B', 'Q'], 'B' : ['C', 'J'], 'Q' : ['R', 'Z'], 'C': ['D', 'G'], 'J': ['K', 'N'],
    'R': ['S', 'V'], 'Z':['Z1', 'Z4'], 'D': ['E', 'F'], 'G':['H', 'I'], 'K':['L', 'M'], 
    'N' : ['O', 'P'], 'S': ['T', 'U'], 'V': ['W', 'X'], 'Z1': ['Z2', 'Z3'], 'Z4': ['Z5', 'Z6']
}

utility = {
    'E':10, 'F':11, 'H':9, 'I':12,'L':14, 'M':15, 'O':13, 'P':14, 'T':15, 'U':2, 
    'W':4, 'X':1, 'Z2':3, 'Z3': 22, 'Z5': 13, 'Z6':14
}

nodes= {}
for node in tree.keys():
    nodes[node] = Node( node )

for node in utility.keys():
    nodes[node] = Node( node )
    nodes[node].utility = utility[node]

def alpha_beta( state ):
    v = max_value( state, float('-inf'), float('inf') )
    return v

def max_value(state, alpha, beta):
    nodes[state].alpha, nodes[state].beta = alpha, beta
    print(f'Exploring {state}')
    if nodes[state].utility != None: return nodes[state].utility
    v = float('-inf')
    for child in tree[state]:
        v = max( v,  min_value(child, nodes[state].alpha, nodes[state].beta))
        pdb.set_trace()
        if v >= nodes[state].beta: 
            print(f'Pruned After {child}. Leaving {state} with {v}')
            return v
        nodes[state].alpha = max( nodes[state].alpha, v  )
    print(f'Leaving {state} with {v}')
    return v

def min_value(state, alpha, beta):
    nodes[state].alpha, nodes[state].beta = alpha, beta
    print(f'Exploring {state}')
    if nodes[state].utility != None: return nodes[state].utility
    v = float('inf')
    for child in tree[state]:
        v = min( v,  max_value(child, nodes[state].alpha, nodes[state].beta))
        pdb.set_trace()
        if v <= nodes[state].alpha:
            print(f'Pruned After {child}. Leaving {state} with {v}')
            return v
        nodes[state].beta = min( nodes[state].beta, v  )
    print(f'Leaving {state} with {v}')
    return v

alpha_beta('A')

pdb.set_trace()