import os
import sys
import subprocess
import time

for i in range(1, 62):
    subprocess.run(["cp", "cases/input_" + str(i) + ".txt", "input.txt"])
    start = time.time()
    subprocess.run(["python3", "homework_resolution.py"])
    total = time.time()-start
    my_output = ''
    with open('output.txt') as ipfile:
        my_output = ipfile.read()
    with open('temp/output_' + str(i) + '.txt', 'w') as temp:
        temp.write(my_output)
    
    theirs = ''
    with open('cases/output_' + str(i) + '.txt') as ipfile:
        theirs = ipfile.read()

    print("Test case", i, ":", my_output.strip() == theirs.strip(), "||  Time Taken ", total)
    #break

