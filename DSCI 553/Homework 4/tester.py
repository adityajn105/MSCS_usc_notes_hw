from sklearn.metrics import normalized_mutual_info_score
from grader import get_list
import os
import sys

#python tester.py data
main = sys.argv[1]

input_output_pairs = [ (f"{main}/test1", f"{main}/cluster1.json", 10), 
                        (f"{main}/test2", f"{main}/cluster2.json", 10), 
                        (f"{main}/test3", f"{main}/cluster3.json", 5), 
                        (f"{main}/test4", f"{main}/cluster4.json", 8), 
                        (f"{main}/test5", f"{main}/cluster5.json", 15), ]

result = []
for dir, test, n_cluster in input_output_pairs:
    for i in range(5):
        os.system(f"python bfr_hb.py {dir} {n_cluster} output.json intermediate.csv")
        ground = get_list(test)
        our = get_list("output.json")

        nmi = normalized_mutual_info_score( ground, our ) 
        result.append( f"{i}, {dir}, {nmi}" )
        print(result[-1])

with open("result.txt", "w") as pf:
    print("\n".join(result), file=pf)