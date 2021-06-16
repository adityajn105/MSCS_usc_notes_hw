import sys
import json

#python task1_grader.py train_review_ground_truth.res task1.res
ground_truth = sys.argv[1].strip()
response = sys.argv[2].strip()

response_set = set()
with open(response, "r") as g:
    for line in g.readlines():
        d = json.loads(line)
        response_set.add( frozenset((d['b1'], d['b2'])) )

ground_set = set()
with open(ground_truth, "r") as g:
    for line in g.readlines():
        d = json.loads(line)
        ground_set.add( frozenset((d['b1'], d['b2'])) )

precision = len(ground_set.intersection(response_set))/len(response_set)
recall = len(ground_set.intersection(response_set))/len(ground_set)

print( f"Precision: {precision} | Recall: {recall}" )

assert precision >= 0.95, "Fail, Precision should be >= 0.95"
assert recall >= 0.5, "Fail, Recall should be >= 0.5"