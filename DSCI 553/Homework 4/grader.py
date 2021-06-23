from sklearn.metrics import normalized_mutual_info_score
import json
import sys

#python grader.py data/cluster2.json output.txt
with open(sys.argv[1], "r") as fp:
    ground_truth = json.loads(fp.read())

with open(sys.argv[2], "r") as fp:
    our_truth = json.loads(fp.read())

size = len(ground_truth)

assert size == len(our_truth), f"Fail: Size of ground truth ({size}) and generated output different ({len(our_truth)}) "

ground = []
our = []
for i in range(size):
    ground.append( ground_truth[str(i)] )
    our.append( our_truth[str(i)] )


nmi = normalized_mutual_info_score( ground, our ) 
print("Normalized Mututal Information", nmi)
assert nmi > 0.8, "Fail: NMI is less than 0.8"