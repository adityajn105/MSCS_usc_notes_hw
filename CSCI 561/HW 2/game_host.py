import random
import os
from time import time, sleep

player1 = "harshita.py"
player2 = "aditi.py"
timep1 = timep2 = max(100, random.random() * 150) #time will be between 100 and 150 seconds
pieces1 = pieces2 = 12

PAUSE = 1

board = [
    [".","b",".","b",".","b",".","b"],
    ["b",".","b",".","b",".","b","."],
    [".","b",".","b",".","b",".","b"],
    [".",".",".",".",".",".",".","."],
    [".",".",".",".",".",".",".","."],
    ["w",".","w",".","w",".","w","."],
    [".","w",".","w",".","w",".","w"],
    ["w",".","w",".","w",".","w","."]
]

column_map = { "a":0, "b":1, "c":2, "d":3, "e":4, "f":5, "g":6, "h":7 }

def verifyMove(typ, start, end, board):
    x1, y1 = start
    x2, y2 = end
    m1, m2 = (x1+x2)//2, (y1+y2)//2
    if abs(x1-x2) != abs(y1-y2): return False
    if typ=='E' and abs(x1-x2) != 1: return False
    if typ=='J' and abs(x1-x2) != 2: return False
    if x1 > 7 or x2 < 0 or y2 > 7 or y2 < 0: return False    
    if typ=='E' and board[x2][y2] != '.': return False
    if typ=='J' and ( board[m1][m2] == '.' 
        or board[m1][m2].lower() == board[x1][y1].lower() 
        or board[x2][y2] != '.' ): return False
    return True

# toss
if random.random() > 0.5:
    player2, player1 = player1, player2
    timep1, timep2 = timep2, timep1   
    pieces1, pieces2 = pieces2, pieces1

white = False
print( f"Each player has {timep1:.2f} seconds to defeat other player.")
print( f"{player1} has won the toss and is playing white." )
if os.path.exists('playdata.txt'): os.remove('playdata.txt');

game_states = dict()
p1_last = p2_last = ""
while True:
    board_str = "\n".join(["".join(line) for line in board])
    with open("host/input.txt", "w") as fp:
        print("GAME", file=fp)
        if white: 
            print("WHITE", file=fp)
            print(f"{timep1:.2f}", file=fp)
        else: 
            print("BLACK", file=fp)
            print(f"{timep2:.2f}", file=fp)
        print(board_str, file=fp)
        game_states[board_str] = game_states.get(board_str,0)+1

    if game_states[board_str] > 3:
        print(f"Game state repeated more than 3 times!! \
{player1 if timep1>timep2 else player2} won by {abs(timep1-timep2):.4f} secs.")
        with open("stats.csv", 'a') as fp: print(f"{player1 if timep1>timep2 else player2},Repeat", file=fp)
        exit()

    if timep1 < 0 or timep2 < 0:
        if timep1 < 0: print(f"Time up, {player2} won by {timep2:.4f} secs.") 
        else: print(f"Time up, {player1} won by {timep1:.4f} secs.")
        with open("stats.csv", 'a') as fp: print(f"{player1 if timep1>timep2 else player2},Timeout", file=fp)
        exit()
    
    if pieces2 == 0 or pieces1 == 0:
        if pieces1 == 0: print(f"Game Finished!! {player2} won.")
        else: print(f"Game Finished!! {player1} won.")
        with open("stats.csv", 'a') as fp: print(f"{player1 if pieces2==0 else player2},Victory", file=fp)
        exit() 

    sleep(PAUSE)
    if white:
        start = time()
        os.system(f"python {player1} host/input.txt  host/output.txt")
        timep1 = timep1 - (time()-start)
    else:
        start = time()
        os.system(f"python {player2} host/input.txt  host/output.txt")
        timep2 = timep2 - (time()-start)
    
    with open("host/output.txt", "r") as fp:
        output = fp.readlines()
        if white:
                if p1_last == "".join(output): print(f'Stalemate - No Moves left!! {player2} wins'); exit()
                else: p1_last = "".join(output)
        else:
                if p2_last == "".join(output): print(f'Stalemate - No Moves left!! {player1} wins'); exit()
                else: p2_last = "".join(output)

        for line in output:
            typ, start, end = tuple(line.split())
            x1, y1 = int(start[1])-1, column_map[ start[0] ]
            x2, y2 = int(end[1])-1, column_map[ end[0] ]
            x1, x2 = 7-x1, 7-x2

            if not verifyMove(typ, (x1, y1), (x2, y2), board):
                print('Invalid Move!!')
                exit()

            if x2==7 or x2==0:
                board[x2][y2] = board[x1][y1].upper()
            else:
                board[x2][y2] = board[x1][y1]
            board[x1][y1] = '.'
            if typ=="J":
                board[(x1+x2)//2][(y1+y2)//2] = '.'
                if white: pieces2 -= 1
                else: pieces1 -= 1
    white = not white