from random import random
from time import process_time as time

max_positions = {(2, 0): True, (2, 2): True, (2, 4): True, (2, 6): True, (1, 1): True, (1, 3): True, (1, 5): True, (1, 7): True} 
min_positions = {(6, 0): True, (6, 2): True, (6, 4): True, (6, 6): True, (4, 0): True, (4, 2): True, (4, 4): True, (4, 6): True}
white = True

def possible_moves( maxs, mins, white_chance = True, restrict_src = None ):
    moves = list()
    start, kill = (restrict_src, True) if restrict_src else (maxs.items(), False)
    for (y, x), typ1 in start:
        for r,c in ( (1,1), (1, -1), (-1, -1), (-1, 1)  ):
            if white_chance and not typ1 and r == -1: continue
            elif not white_chance and not typ1 and r == 1: continue
            ny, nx = y+r, x+c
            if ny in (-1, 8) or nx in (-1, 8): continue
            elif (ny, nx) in maxs: continue
            elif (ny, nx) in mins:
                new_p = (ny+r, nx+c)
                if new_p[0] not in (-1, 8) and new_p[1] not in (-1, 8) and \
                    new_p not in maxs and new_p not in mins:
                    if not kill: moves.clear(); kill = True
                    moves.append( ((y,x), new_p, True) )
                continue
            elif not kill: moves.append( ( (y,x), (ny, nx), False) )
            else: continue
    return moves, kill and len(moves) > 0

def evaluate(maxs, mins):
    value  = random()/5
    for _, k in maxs.items(): value += (7 + 3*k)
    for _, k in mins.items(): value -= (7 + 3*k)
    return value
    
def getNewPositions( maxs, mins, old_p, new_p, kill, white_chance ):
    new_maxs, new_mins = maxs.copy(), mins.copy()
    new_maxs[new_p] = new_maxs.pop( old_p )
    if kill: new_mins.pop( ( (old_p[0]+new_p[0])//2, (old_p[1]+new_p[1])//2 ) )
    
    king = False
    if white_chance and new_p[0] == 7 and new_maxs[new_p] == 0: 
        new_maxs[new_p], king = 1, True
    elif not white_chance and new_p[0] == 0 and new_maxs[new_p] == 0: 
        new_maxs[new_p], king = 1, True
    else: pass
    return new_maxs, new_mins, king

def max_value(maxs, mins, alpha, beta, depth, white_chance, restrict_src_for_kill = None):
    if depth >= CUTTOFF_THRESHOLD: return evaluate( maxs, mins )
    v = float('-inf')
    global OPT_ACTION; 
    
    moves = None
    if restrict_src_for_kill:
        moves, kill =  possible_moves(maxs, mins, white_chance, 
                            restrict_src=[ (restrict_src_for_kill, maxs[restrict_src_for_kill]) ])
        if not kill: return min_value(mins, maxs, alpha, beta, depth, not white_chance)
    else: moves, kill = possible_moves(maxs, mins, white_chance)
    if not moves: return evaluate(maxs, mins)

    for old_p, new_p, kill in moves:
        new_maxs, new_mins, king = getNewPositions( maxs, mins, old_p, new_p, kill, white_chance )
        if kill and not king:
            nv = max_value( new_maxs, new_mins, alpha, beta, depth+1, white_chance, restrict_src_for_kill=new_p)
            if nv > v:
                if not depth: OPT_ACTION = (old_p, new_p)
                v = nv
        else:
            nv = min_value(new_mins, new_maxs, alpha, beta, depth+1, not white_chance)
            if nv > v:
                if not depth: OPT_ACTION = (old_p, new_p)
                v = nv

        if v >= beta: return v
        alpha = max( alpha, v  )
    return v

def min_value(maxs, mins, alpha, beta, depth, white_chance, restrict_src_for_kill = None):
    if depth >= CUTTOFF_THRESHOLD: return evaluate( mins, maxs )
    v = float('inf')

    moves = None
    if restrict_src_for_kill:
        moves, kill = possible_moves(maxs, mins, white_chance,
                            restrict_src=[ (restrict_src_for_kill, maxs[restrict_src_for_kill]) ])
        if not kill: return max_value(mins, maxs, alpha, beta, depth, not white_chance)
    else: moves, kill = possible_moves(maxs, mins, white_chance)
    if not moves: return evaluate(mins, maxs)

    for old_p, new_p, kill in moves:
        new_maxs, new_mins, king = getNewPositions( maxs, mins, old_p, new_p, kill, white_chance )
        if kill and not king:
            v = min( v,  min_value( new_maxs, new_mins, alpha, beta, depth+1, white_chance, restrict_src_for_kill=new_p)) 
        else:
            v = min( v,  max_value( new_mins, new_maxs, alpha, beta, depth+1, not white_chance ))
        if v <= alpha: return v
        beta = min( beta, v  )
    return v

time_elapsed = 0
for depth in range(1, 11):
    CUTTOFF_THRESHOLD = depth
    start = time()
    max_value( max_positions, min_positions, float('-inf'), float('inf'), 0, white )
    print(depth, time()-start)
    time_elapsed += (time()-start)
print(time_elapsed)
with open("calibration.txt", "w") as fp:
    print(time_elapsed, file=fp, end="")

# calibrate_time = 0
# try: 
#     with open('calibration.txt', 'r') as fp: calibrate_time = float(fp.readline())
# except: passtime