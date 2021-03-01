import pdb
from collections import deque
from time import time

inp = open("cases/input.txt", "r")
single = inp.readline().strip() == 'SINGLE'
white = inp.readline().strip() == 'WHITE'
remain_time = float(inp.readline().strip())

board = []
for i in range(8):
    board.append( inp.readline().strip() )

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

if not white:
    min_positions, max_positions = max_positions, min_positions

def possible_moves( maxs, mins, white_chance = True ):
    moves = deque()#Heap([])
    value = -1*evaluate(maxs, mins)
    for (y, x), typ1 in maxs.items():
        for r,c in ( (1,1), (1, -1), (-1, -1), (-1, 1)  ):
            if white_chance and r == -1 and typ1 != 1: continue
            elif not white_chance and r == 1 and typ1 != 1: continue
            ny, nx = y+r, x+c
            if ny in (-1, 8) or nx in (-1, 8): continue
            elif (ny, nx) in maxs: continue
            elif (ny, nx) in mins:
                if (ny+r) not in (-1, 8) and (nx+c) not in (-1, 8) and \
                    ( (ny+r), (nx+c) ) not in maxs and ( (ny+r), (nx+c) ) not in mins:
                    #kill moves can increse chance of pruning, so exploring first
                    profit = 5 if mins[(ny, nx)] == 1 else 3
                    moves.appendleft( ((y,x), (ny+r, nx+c), True) )
                else: continue
            else: moves.append( ( (y,x), (ny, nx), False) )
    return moves

def restrictedMove( maxs, mins, white_chance, pos, typ1 ):
    moves = deque()
    y, x = pos
    for r,c in ( (1,1), (1, -1), (-1, -1), (-1, 1)  ):
            if white_chance and r == -1 and typ1 != 1: continue
            elif not white_chance and r == 1 and typ1 != 1: continue
            ny, nx = y+r, x+c
            if ny in (-1, 8) or nx in (-1, 8): continue
            elif (ny, nx) in maxs: continue
            elif (ny, nx) in mins:
                if (ny+r) not in (-1, 8) and (nx+c) not in (-1, 8) and \
                    ( (ny+r), (nx+c) ) not in maxs and ( (ny+r), (nx+c) ) not in mins:
                    #kill moves can increse chance of pruning, so exploring first
                    profit = 5 if mins[(ny, nx)] == 1 else 3
                    moves.appendleft( ((y,x), (ny+r, nx+c), True) )
                else: continue
            else: continue
    return moves

def evaluate( maxs, mins ):
    no_of_min_kings = sum(mins.values())
    no_of_max_kings = sum(maxs.values())
    no_of_min_pieces = len(mins) - no_of_min_kings
    no_of_max_pieces = len(maxs) - no_of_max_kings
    return 5*no_of_max_kings + 3*no_of_max_pieces - 5*no_of_min_kings - 3*no_of_min_pieces 

CUTTOFF_THRESHOLD = 8
OPT_ACTION  = None

def getNewPositions( maxs, mins, old_p, new_p, kill, white_chance ):
    new_maxs, new_mins = maxs.copy(), mins.copy()
    new_maxs[new_p] = new_maxs.pop( old_p )
    if kill: new_mins.pop( ( (old_p[0]+new_p[0])//2, (old_p[1]+new_p[1])//2 ) )

    if white_chance and new_p[0] == 7: new_maxs[new_p] = 1
    elif not white_chance and new_p[0] == 0: new_maxs[new_p] = 1
    return new_maxs, new_mins

def max_value(maxs, mins, alpha, beta, depth, white_chance):
    if depth >= CUTTOFF_THRESHOLD: return evaluate( maxs, mins )
    global OPT_ACTION
    v = float('-inf')
    for old_p, new_p, kill in possible_moves(maxs, mins, white_chance):
        if kill:
            new_maxs, new_mins = getNewPositions( maxs, mins, old_p, new_p, kill, white_chance )
            for temp_old_p, temp_new_p, kill2 in restrictedMove(new_maxs, new_mins, white_chance, new_p, maxs[old_p]):
                temp_new_maxs, temp_new_mins = getNewPositions( new_maxs, new_mins, temp_old_p, temp_new_p, kill2, white_chance )
                nv = max_value( temp_new_maxs, temp_new_mins, alpha, beta, depth+1, white_chance )
                if nv > v:
                    if depth==0: 
                        OPT_ACTION = (old_p, new_p)
                    v = nv
        if v >= beta: return v
        alpha = max( alpha, v  )
        
        new_maxs, new_mins = getNewPositions( maxs, mins, old_p, new_p, kill, white_chance )
        if depth != 0: v = max( v,  min_value(new_mins, new_maxs, alpha, beta, depth+1, not white_chance))
        else: 
            nv = min_value(new_mins, new_maxs, alpha, beta, depth+1, not white_chance)
            if nv > v: 
                OPT_ACTION, v = (old_p, new_p), nv
        if v >= beta: return v
        alpha = max( alpha, v  )
    return v

def min_value(maxs, mins, alpha, beta, depth, white_chance):
    if depth >= CUTTOFF_THRESHOLD: return evaluate( mins, maxs ) #we see value of board for us
    v = float('inf')
    for old_p, new_p, kill in possible_moves(maxs, mins, white_chance):
        if kill:
            new_maxs, new_mins = getNewPositions( maxs, mins, old_p, new_p, kill, white_chance )
            for temp_old_p, temp_new_p, kill2 in restrictedMove(new_maxs, new_mins, white_chance, new_p, maxs[old_p]):
                temp_new_maxs, temp_new_mins = getNewPositions( new_maxs, new_mins, temp_old_p, temp_new_p, kill2, white_chance )
                v = min( v,  min_value( temp_new_maxs, temp_new_mins, alpha, beta, depth+1, white_chance ))
        if v <= alpha: return v
        beta = min( beta, v  )

        new_maxs, new_mins = getNewPositions( maxs, mins, old_p, new_p, kill, white_chance )
        v = min( v,  max_value( new_mins, new_maxs, alpha, beta, depth+1, not white_chance ))
        if v <= alpha: return v
        beta = min( beta, v  )
    return v

#print( min_positions, possible_moves(max_positions, min_positions, True) )
#print(evaluate(max_positions, min_positions))
start = time()
v = max_value( max_positions, min_positions, float('-inf'), float('inf'), 0, white )
print( f"{v} {OPT_ACTION} {time()-start}" )

fp = open('output.txt', 'w')
column_map = { 0:"a", 1:"b", 2:"c", 3:"d", 4:"e", 5:"f", 6:"g", 7:"h" }
(x1, y1), (x2, y2) = OPT_ACTION
if abs(x1-x2) > 1:
    print(f"J {column_map[x1]}{y1+1} {column_map[x2]}{y2+1}", file=fp)
else:
    print(f"E {column_map[x1]}{y1+1} {column_map[x2]}{y2+1}", file=fp)
fp.close()