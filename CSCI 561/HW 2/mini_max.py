from collections import deque
import random
import sys
import time

inp = open(sys.argv[1], "r")
single = inp.readline().strip() == 'SINGLE'
white = inp.readline().strip() == 'WHITE'
remain_time = float(inp.readline().strip())

board = []
for i in range(8):
    board.append( inp.readline().strip() )

# def performance(maxs, mins, depth, white):
#     if depth == 0: return 1
#     nodes = 0
#     moves, _ = possible_moves(maxs, mins)
#     for old_p, new_p, kill in moves:
#         new_maxs, new_mins, _ = getNewPositions( maxs, mins, old_p, new_p, kill, white)
#         nodes += performance(new_maxs, new_mins, depth-1, not white)
#     return nodes

# def get_nps(maxs, mins, depth=5):
#     start_time = time.time()
#     nodes = performance(maxs, mins, depth, True)
#     end_time = time.time()
#     print("NPS:", nodes / (end_time - start_time))

max_positions = {}
min_positions = {}
for i in range(8):
    y = 7-i
    for x in range(8):
        c = board[i][x] 
        if c == '.': continue
        elif c=='w': max_positions[ (y,x) ] = 0
        elif c=='W': max_positions[ (y,x) ] = 1
        elif c=='b': min_positions[ (y,x) ] = 0
        elif c=='B': min_positions[ (y,x) ] = 1
        else: continue

if not white: min_positions, max_positions = max_positions, min_positions

def possible_moves( maxs, mins, white_chance = True, restrict_src = None ):
    moves = deque()
    kill = False
    
    start = maxs.items()
    if restrict_src != None:
        start = restrict_src
    
    for (y, x), typ1 in start:
        for r,c in ( (1,1), (1, -1), (-1, -1), (-1, 1)  ):
            if white_chance and r == -1 and typ1 != 1: continue
            elif not white_chance and r == 1 and typ1 != 1: continue
            ny, nx = y+r, x+c
            if ny in (-1, 8) or nx in (-1, 8): continue
            elif (ny, nx) in maxs: continue
            elif (ny, nx) in mins:
                new_p = (ny+r, nx+c)
                if new_p[0] not in (-1, 8) and new_p[1] not in (-1, 8) and \
                    new_p not in maxs and new_p not in mins:
                    moves.appendleft( ((y,x), new_p, True) )
                    kill = True
                else: continue
            else: moves.append( ( (y,x), (ny, nx), False) )
    #force jump
    if kill: moves = list( filter(lambda x: x[2], moves) )
    return moves, kill

def evaluate( maxs, mins ):
    return len(maxs) - len(mins)

CUTTOFF_THRESHOLD = 3
OPT_ACTION  = None


def getNewPositions( maxs, mins, old_p, new_p, kill, white_chance ):
    new_maxs, new_mins = maxs.copy(), mins.copy()
    new_maxs[new_p] = new_maxs.pop( old_p )
    if kill: new_mins.pop( ( (old_p[0]+new_p[0])//2, (old_p[1]+new_p[1])//2 ) )
    
    king = False
    if new_p[0] == 7 and new_maxs[new_p] == 0 and white_chance: 
        new_maxs[new_p], king = 1, True
    elif new_p[0] == 0 and new_maxs[new_p] == 0 and not white_chance: 
        new_maxs[new_p], king = 1, True
    else: pass

    return new_maxs, new_mins, king

def max_value(maxs, mins, depth, white_chance, restrict_src_for_kill = None):
    if depth >= CUTTOFF_THRESHOLD: return evaluate(maxs, mins)
    global OPT_ACTION;
    v = float('-inf')
    
    moves = []
    if restrict_src_for_kill != None:
        moves, kill =  possible_moves(maxs, mins, white_chance, 
                            restrict_src=[ (restrict_src_for_kill, maxs[restrict_src_for_kill]) ])
        if not kill: return min_value(mins, maxs, depth, not white_chance)
    else: moves, kill = possible_moves(maxs, mins, white_chance)
    if len(moves) == 0: return evaluate(maxs, mins)

    for old_p, new_p, kill in moves:
        new_maxs, new_mins, king = getNewPositions( maxs, mins, old_p, new_p, kill, white_chance )
        if kill and not king:
            nv = max_value( new_maxs, new_mins, depth+1, white_chance, restrict_src_for_kill=new_p)
            if nv > v:
                if depth == 0: OPT_ACTION = (old_p, new_p)
                v = nv
        else:
            nv = min_value(new_mins, new_maxs, depth+1, not white_chance)
            if nv > v:
                if depth == 0: OPT_ACTION = (old_p, new_p)
                v = nv
    return v

def min_value(maxs, mins, depth, white_chance, restrict_src_for_kill = None):
    if depth >= CUTTOFF_THRESHOLD: return evaluate( mins, maxs ) #we see value of board for us
    v = float('inf')

    moves = []
    if restrict_src_for_kill != None:
        moves, kill = possible_moves(maxs, mins, white_chance, 
                            restrict_src=[ (restrict_src_for_kill, maxs[restrict_src_for_kill]) ])
        if not kill: return max_value(mins, maxs, depth, not white_chance)
    else: moves, kill = possible_moves(maxs, mins, white_chance)
    if len(moves) == 0: return evaluate(mins, maxs)

    for old_p, new_p, kill in moves:
        new_maxs, new_mins, king = getNewPositions( maxs, mins, old_p, new_p, kill, white_chance )
        if kill and not king:
            v = min( v,  min_value( new_maxs, new_mins, depth+1, white_chance, restrict_src_for_kill=new_p)) 
        else:
            v = min( v,  max_value( new_mins, new_maxs, depth+1, not white_chance ))
    return v

v = max_value( max_positions, min_positions, 0, white )

#get_nps(max_positions, min_positions, 5)

fp = open(sys.argv[2], 'w')
column_map = { 0:"a", 1:"b", 2:"c", 3:"d", 4:"e", 5:"f", 6:"g", 7:"h" }
(x1, y1), (x2, y2) = OPT_ACTION

if abs(x1-x2) > 1:
    moves_list = []
    while abs(x1-x2) > 1:
        moves_list.append(f"J {column_map[y1]}{x1+1} {column_map[y2]}{x2+1}")    
        max_positions, min_positions, king = getNewPositions( max_positions, min_positions, (x1, y1), (x2, y2), True, white )
        if king: break
        OPT_ACTION  = None
        v = max_value(max_positions, min_positions, 0, white, restrict_src_for_kill=(x2, y2) )
        if OPT_ACTION == None: break
        (x1, y1), (x2, y2) = OPT_ACTION
    print( "\n".join(moves_list), file=fp)
else:
    print(f"E {column_map[y1]}{x1+1} {column_map[y2]}{x2+1}", file=fp)
fp.close()