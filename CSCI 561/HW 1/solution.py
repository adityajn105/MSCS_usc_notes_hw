from queue import deque
from time import time
import pdb 

inp = open("case5/input.txt", "r")

algo = inp.readline().strip()
W,H = tuple(map(int, inp.readline().strip().split())) #width, height
x,y = tuple(map(int, inp.readline().strip().split()[::-1])) #starting position
h = int(inp.readline().strip()) #height difference that wagon can climb
N = int(inp.readline().strip()) #number of settling sites
targets = []
for _ in range(N):
    targets.append( tuple(map(int, inp.readline().strip().split()[::-1])) )
terrain = []
for _ in range(H):
    terrain.append( list(map(int, inp.readline().strip().split())) )
inp.close()

# targets = []
# for i in range(H):
#     for j in range(W):
#         targets.append( (i,j) )

#targets= [(1,4)]

class Heap():
    def __init__(self, arr=[], key=lambda x: x, value=lambda x: x):
        self.arr = arr
        self.key = key
        self.value = value
        self.hashtable = dict()
        for i in range(len(self)):
            self.hashtable[ self.__getValue(i) ] = i
        self.heapify()
        
    def __repr__(self): return f"{self.arr}"
    def __len__(self): return len(self.arr)
    def __getitem__(self, i): return self.key( self.arr[i] )
    
    def peek(self): return self.arr[0]
    def isEmpty(self): return len(self)==0
    
    def __swap_hash_indices(self, i,j):
        self.hashtable[ self.__getValue(i) ], self.hashtable[ self.__getValue(j) ] = \
            self.hashtable[ self.__getValue(j) ], self.hashtable[ self.__getValue(i) ]

    def __getValue(self, i):
        return self.value(self.arr[i])

    def getItem(self, val):
        return self.arr[self.hashtable[val]]

    def checkVal(self, val):
        return val in self.hashtable

    def heapify(self):
        for i in range( (len(self)+1)//2, -1, -1 ):
            self.__bubble_down(i)

    def insert(self, val):
        self.arr.append(val)
        self.hashtable[ self.__getValue(len(self)-1) ] = len(self)-1
        self.__bubble_up(len(self)-1)

    def extractMin(self):
        self.arr[0], self.arr[-1] = self.arr[-1], self.arr[0]
        self.__swap_hash_indices(0, len(self)-1)
        self.hashtable.pop( self.__getValue(len(self)-1) )
        ans = self.arr.pop()
        self.__bubble_down(0)
        return ans

    def updateKey(self, val):
        key = self.key(val)
        idx = self.hashtable[ self.value(val) ]
        old_key = self[idx]
        self.arr[idx] = val
        
        if key < old_key: self.__bubble_up(idx)
        elif key > old_key: self.__bubble_down(idx)
        else: pass

    def __bubble_up(self, idx):
        parent = (idx-1)//2
        while parent >=0 and self[idx] < self[parent]:
            self.__swap_hash_indices(idx, parent)
            self.arr[idx], self.arr[parent] = self.arr[parent], self.arr[idx]
            idx, parent = parent, (parent-1)//2
        
    def __bubble_down(self, idx):
        high = len(self)
        while True:
            l, r = (2*idx)+1, (2*idx)+2
            if r<high:
                if self[l] > self[r]:
                    if self[r] >= self[idx]: break
                    self.__swap_hash_indices( idx, r )
                    self.arr[idx], self.arr[r] = self.arr[r], self.arr[idx]
                    idx = r
                else:
                    if self[l] >= self[idx]: break
                    self.__swap_hash_indices( idx, l )
                    self.arr[idx], self.arr[l] = self.arr[l], self.arr[idx]
                    idx = l
            elif l<high:
                if self[l] >= self[idx]: break
                self.__swap_hash_indices( idx, l )
                self.arr[idx], self.arr[l] = self.arr[l], self.arr[idx]
                idx = l
            else:
                break

def positions_bfs(current):
    r,c = current
    ch = min(terrain[r][c], 0)
    position_list = []
    for rc, cc in [ (1,1), (-1,-1), (1, -1), (-1, 1), (1,0), (0,1), (-1, 0), (0, -1) ] :
        nr, nc = r+rc, c+cc
        if nr > H-1 or nr < 0 or nc > W-1 or nc < 0: continue
        nh = min(terrain[nr][nc], 0)
        if abs(nh-ch) > h: continue
        else: position_list.append( (nr, nc) )
    return position_list
    
def bfs(start, goal):
    if start==goal: return None, {start: -1}
    dq = deque()
    dq.append( start )
    visited = { start : True }
    paths = { start: -1 }
    while len(dq)!=0:
        pos = dq.popleft()
        position_list = positions_bfs(pos)
        for position in position_list:
            if position==goal: paths[ position ]  = pos; return None, paths
            elif not visited.get(position, False):
                dq.append(position)
                paths[ position ]  = pos
                visited[ position ] = True
            else: continue
    return None, paths

def positions_ucs(current):
    r,c = current
    ch = min(terrain[r][c], 0)
    position_list = []
    for rc, cc, cost in [ (1,1, 14), (-1,-1, 14), (1, -1, 14), (-1, 1, 14), 
                    (1,0, 10), (0,1, 10), (-1, 0, 10), (0, -1, 10) ] :
        nr, nc = r+rc, c+cc
        if nr > H-1 or nr < 0 or nc > W-1 or nc < 0: continue
        nh = min(terrain[nr][nc], 0)
        if abs(nh-ch) > h: continue
        else: position_list.append( ((nr, nc), cost) )
    return position_list

def UCS(start, goal):
    q_open = Heap([], key=lambda x: x[0], value=lambda x: x[1])
    q_open.insert( (0, start) )
    closed = set()
    paths = { start: -1 }
    while len(q_open) != 0:
        cost, pos = q_open.extractMin()
        if pos==goal: return cost, paths
        closed.add( pos )
        position_list = positions_ucs(pos)
        for position, cst in position_list:
            if position not in closed and not q_open.checkVal(position):
                q_open.insert( (cost+cst, position) )
                paths[position] = pos
            elif q_open.checkVal(position):
                c, p = q_open.getItem(position)
                if p==position and c > cst+cost:
                    q_open.updateKey( (cst+cost, p) )
                    paths[position] = pos
            else: continue
    return None, paths

def positions_astar(current):
    r,c = current
    ch = min(terrain[r][c], 0)
    position_list = []
    for rc, cc, cost in [ (1,1, 14), (-1,-1, 14), (1, -1, 14), (-1, 1, 14), 
                    (1,0, 10), (0,1, 10), (-1, 0, 10), (0, -1, 10) ]:
        nr, nc = r+rc, c+cc
        if nr > H-1 or nr < 0 or nc > W-1 or nc < 0: continue
        nh = min(terrain[nr][nc], 0)
        if abs(nh-ch) > h: continue
        else: position_list.append( ( (nr, nc), cost+abs(ch-nh)+max(0, terrain[nr][nc]) ))
    return position_list

def euclidean( coord1, coord2 ):
    x1, y1 = coord1
    x2, y2 = coord2
    return 10*(( (x1-x2)**2 + (y1-y2)**2 )**0.5)

def astar(start, goal):
    q_open = Heap([], key=lambda x: x[0], value=lambda x: x[2])
    q_open.insert( (0+euclidean(start, goal), 0, start) )
    closed = set()
    paths = { start: -1 }
    while len(q_open) != 0:
        cost, gn, pos = q_open.extractMin()
        if pos == goal: return cost, paths
        closed.add( pos )
        position_list = positions_astar(pos)
        for position, cst in position_list:
            if position not in closed and not q_open.checkVal(position):
                fn = (gn+cst) + euclidean(position, goal)
                q_open.insert( (fn, gn+cst, position) )
                paths[position] = pos
            elif q_open.checkVal(position):
                c,g,p = q_open.getItem( position )
                if p==position and g > gn+cst:
                    q_open.updateKey( (gn+cst+euclidean(position, goal), cst+gn, p)  ) 
                    paths[position] = pos
            else: continue
    return None, paths

def getPath(paths, gol):
    path = []
    while gol!=-1:
        path.append(gol)
        gol = paths[gol]
    return path[::-1]

fp = open(f"output.txt", "w")
algo_map = { "BFS":bfs, "UCS": UCS, "A*": astar }
for goal in targets:
    cost, path = algo_map[algo]((x,y), goal)
    if goal not in path: print('FAIL', file=fp)
    else: print( str(cost)+" "+str(len(getPath(path, goal)))+" : "+" ".join([ f"{c},{r}" for r,c in getPath(path, goal) ]), file=fp)
fp.close()