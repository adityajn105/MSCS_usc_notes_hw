from time import time
import os

inp = open("host/test.txt", "r")
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

def performance(maxs, mins, depth, white):
    if depth == 0: return 1
    nodes = 0
    moves, _ = possible_moves(maxs, mins)
    for old_p, new_p, kill in moves:
        new_maxs, new_mins, _ = getNewPositions( maxs, mins, old_p, new_p, kill, white)
        nodes += performance(new_maxs, new_mins, depth-1, white)
    return nodes

def get_nps(maxs, mins, depth=5):
    start_time = time()
    nodes = performance(maxs, mins, depth, True)
    end_time = time()
    return nodes / (end_time - start_time)
    
mn = 0 
for _ in range(1):
    mn += get_nps( max_positions, min_positions )
print("NPS :", mn)