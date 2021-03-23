from multiprocessing import Pool
import random
import os
from time import time

player1 = "homework3.py"
player2 = "mini_max.py"

def run_game(args):
    num, player1, player2 = args
    input_file = "tmp/input-%d.txt" % num
    output_file = "tmp/output-%d.txt" % num

    timep1 = timep2 = 150  # time will be between 100 and 150 seconds
    pieces1 = pieces2 = 12

    #toss
    if random.random() > 0.5:
        player2, player1 = player1, player2
        timep1, timep2 = timep2, timep1
        pieces1, pieces2 = pieces2, pieces1

    board = [
        [".", "b", ".", "b", ".", "b", ".", "b"],
        ["b", ".", "b", ".", "b", ".", "b", "."],
        [".", "b", ".", "b", ".", "b", ".", "b"],
        [".", ".", ".", ".", ".", ".", ".", "."],
        [".", ".", ".", ".", ".", ".", ".", "."],
        ["w", ".", "w", ".", "w", ".", "w", "."],
        [".", "w", ".", "w", ".", "w", ".", "w"],
        ["w", ".", "w", ".", "w", ".", "w", "."]
    ]

    column_map = {"a": 0, "b": 1, "c": 2, "d": 3, "e": 4, "f": 5, "g": 6, "h": 7}

    def verifyMove(typ, start, end, board):
        x1, y1 = start
        x2, y2 = end
        m1, m2 = (x1 + x2) // 2, (y1 + y2) // 2
        if abs(x1 - x2) != abs(y1 - y2): return False
        if typ == 'E' and abs(x1 - x2) != 1: return False
        if typ == 'J' and abs(x1 - x2) != 2: return False
        if x1 > 7 or x2 < 0 or y2 > 7 or y2 < 0: return False
        if typ == 'E' and board[x2][y2] != '.': return False
        if typ == 'J' and (board[m1][m2] == '.'
                           or board[m1][m2].lower() == board[x1][y1].lower()
                           or board[x2][y2] != '.'): return False
        return True

    white = False
    print(f"Each player has {timep1:.2f} seconds to defeat other player.")

    game_states = dict()
    p1_last = p2_last = ""
    try:
        while True:
            board_str = "\n".join(["".join(line) for line in board])
            with open(input_file, "w") as fp:
                print("GAME", file=fp)
                if white:
                    print("WHITE", file=fp)
                    print(f"{timep1:.2f}", file=fp)
                else:
                    print("BLACK", file=fp)
                    print(f"{timep2:.2f}", file=fp)
                print(board_str, file=fp)
                game_states[board_str] = game_states.get(board_str, 0) + 1

            if game_states[board_str] > 3:
                print(f"Game state repeated more than 3 times!! \
        {player1 if timep1 > timep2 else player2} won by {abs(timep1 - timep2):.4f} secs.")
                with open("stats-match.csv", 'a') as fp: print(f"{player1 if timep1 > timep2 else player2},Repeat",
                                                              file=fp)
                winner = timep1 > timep2
                raise EOFError()

            if timep1 < 0 or timep2 < 0:
                if timep1 < 0:
                    print(f"Time up, {player2} won by {timep2:.4f} secs.")
                else:
                    print(f"Time up, {player1} won by {timep1:.4f} secs.")
                with open("stats-match.csv", 'a') as fp:
                    print(f"{player1 if timep1 > timep2 else player2},Timeout", file=fp)
                winner = timep1 > timep2
                raise EOFError()

            if pieces2 == 0 or pieces1 == 0:
                if pieces1 == 0:
                    print(f"Game Finished!! {player2} won.")
                else:
                    print(f"Game Finished!! {player1} won.")
                with open("stats-match.csv", 'a') as fp:
                    print(f"{player1 if pieces2 == 0 else player2},Victory", file=fp)
                winner = pieces2 == 0
                raise EOFError()

            start = time()
            if white:
                os.system(f"python {player1} {input_file} {output_file}")
                timep1 = timep1 - (time() - start)
            else:
                os.system(f"python {player2} {input_file} {output_file}")
                timep2 = timep2 - (time() - start)

            with open(output_file, "r") as fp:
                output = fp.readlines()
                if white:
                        if p1_last == "".join(output): 
                            print(f'Stalemate - No Moves left!! {player2} wins');
                            with open("stats-match.csv", 'a') as fp: print(f"{player2},Stalemate",file=fp)
                            raise EOFError() 
                        else: p1_last = "".join(output)
                else:
                        if p2_last == "".join(output): 
                            print(f'Stalemate - No Moves left!! {player1} wins'); 
                            with open("stats-match.csv", 'a') as fp: print(f"{player1},Stalemate",file=fp)
                            raise EOFError()
                        else: p2_last = "".join(output)
                for line in output:
                    typ, start, end = tuple(line.split())
                    x1, y1 = int(start[1]) - 1, column_map[start[0]]
                    x2, y2 = int(end[1]) - 1, column_map[end[0]]
                    x1, x2 = 7 - x1, 7 - x2

                    if not verifyMove(typ, (x1, y1), (x2, y2), board):
                        print('Invalid Move!!')
                        if white: 
                            print(f'Stalemate - No Moves left!! {player2} wins'); 
                            with open("stats-match.csv", 'a') as fp: print(f"{player2},Stalemate",file=fp)
                        else: 
                            print(f'Stalemate - No Moves left!! {player1} wins'); 
                            with open("stats-match.csv", 'a') as fp: print(f"{player1},Stalemate",file=fp)
                        raise EOFError()

                    if x2 == 7 or x2 == 0:
                        board[x2][y2] = board[x1][y1].upper()
                    else:
                        board[x2][y2] = board[x1][y1]
                    board[x1][y1] = '.'
                    if typ == "J":
                        board[(x1 + x2) // 2][(y1 + y2) // 2] = '.'
                        if white:
                            pieces2 -= 1
                        else:
                            pieces1 -= 1
            white = not white
    except KeyboardInterrupt:
        exit()
    except EOFError:
        pass
    # break


if __name__ == '__main__':
    pairs = []
    for i in range(50):
        pairs.append((i, player1, player2))
        # break
    try:
        with Pool(processes=6) as pool:
            done = 0
            for _ in pool.imap_unordered(run_game, pairs):
                done += 1
                print("Done: %d" % done)
    except KeyboardInterrupt:
        exit()


# import json
# data = json.load(open('csci561/hw2/weights.json'))
# data['lost'] = []
# data['left'].extend(data['won'])
# print(len(data['left']))
# data['won'] = []
# print(len(data['won']))
# json.dump(data, open('csci561/hw2/weights.json', 'w'))
