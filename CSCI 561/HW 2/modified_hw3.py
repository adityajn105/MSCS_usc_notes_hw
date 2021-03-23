from random import shuffle
import sys

inp = open(sys.argv[1], "r")
single = inp.readline().strip() == 'SINGLE'
white = inp.readline().strip() == 'WHITE'
remain_time = float(inp.readline().strip())
board = []
for i in range(8):
    board.append( inp.readline().lstrip() )
inp.close()

max_positions = {}
min_positions = {}
for i in range(8):
    y = 7-i
    for x in range(8):
        c = board[i][x] 
        if c == '.': continue
        elif c=='w': max_positions[ (y,x) ] = False
        elif c=='W': max_positions[ (y,x) ] = True
        elif c=='b': min_positions[ (y,x) ] = False
        elif c=='B': min_positions[ (y,x) ] = True
        else: continue

if not white: min_positions, max_positions = max_positions, min_positions

CUTTOFF_THRESHOLD = 6
OPT_ACTION  = None

if not single: 
    pieces = len(max_positions)+len(min_positions)
    CUTTOFF_THRESHOLD = (9, 9, 7, 10, 10, 10, 10, 10, 9, 9, 9, 9, 9, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7)[pieces]

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
    shuffle(moves)
    return moves, kill and len(moves) > 0

def evaluate(maxs, mins):
    value = 0
    for _, k in maxs.items(): value += (7 + 10*k)
    for _, k in mins.items(): value -= (7 + 10*k)
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
    
    moves = set()
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

    moves = set()
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

max_value( max_positions, min_positions, float('-inf'), float('inf'), 0, white )

fp = open(sys.argv[2], 'w')
column_map = ["a", "b", "c", "d", "e", "f", "g", "h"]
(x1, y1), (x2, y2) = OPT_ACTION

if abs(x1-x2) > 1:
    CUTTOFF_THRESHOLD = 4
    moves_list = []
    while abs(x1-x2) > 1:
        moves_list.append(f"J {column_map[y1]}{x1+1} {column_map[y2]}{x2+1}")    
        max_positions, min_positions, king = getNewPositions( max_positions, min_positions, (x1, y1), (x2, y2), True, white )
        if king: break
        OPT_ACTION  = None
        max_value(max_positions, min_positions, float('-inf'), float('inf'), 0, white, restrict_src_for_kill=(x2, y2) )
        if OPT_ACTION == None: break
        (x1, y1), (x2, y2) = OPT_ACTION
    print( "\n".join(moves_list), file=fp)
else:
    print(f"E {column_map[y1]}{x1+1} {column_map[y2]}{x2+1}", file=fp)
fp.close()